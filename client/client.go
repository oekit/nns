package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oekit/concurrency/singleflight"
	"github.com/oekit/logx"
	"github.com/oekit/nns/metadata"
	"github.com/oekit/registry"
)

var nnsLog = logx.Delegating("nns")

// ErrRegistrarUnavailable 历史上在 ServiceClient.Register 被调用但未配置
// registrar 时返回。常见场景：未注入 registrar、Mock 模式未启用、provider
// 构造失败降级。
//
// #66 起「没注册器」改为幂等 no-op success，本哨兵**不再被任何入口返回**
// （全仓零返回点），仅为兼容保留符号：消费方照旧编译不报错，但
// errors.Is(err, ErrRegistrarUnavailable) 分支永不命中，不应再写依赖它的
// 错误分叉（判「没注册器」请直接看 MockMode()/Registrar()）。
var ErrRegistrarUnavailable = errors.New("nns: registrar not available")

// ErrServiceNameRequired 在显式指定服务名的注册/注销入口（RegisterAs /
// DeregisterAs）拿到空服务名时返回。
var ErrServiceNameRequired = errors.New("nns: service name required")

// ErrClientClosed 在 ServiceClient.Close() 之后调用订阅类方法时返回。
// Close 是终结操作（v0.28.0 起），复用需重新 New。
var ErrClientClosed = errors.New("nns: client closed")

// ServiceClient NNS 服务核心客户端，管理服务发现、配置、实例缓存和订阅。
//
// 依赖 registry 包定义的 4 个接口（Registrar/Discoverer/Subscriber/ConfigStore），
// 不直接耦合任何具体注册中心 SDK：provider 经 With* 选项注入，未注入即
// 降级态（mock/静态实例/缓存兜底）；Nacos 默认构造由 nns 根门面承担。
type ServiceClient struct {
	mu sync.RWMutex

	localService     *metadata.ServiceBasic
	localServiceMeta atomic.Pointer[map[string]string]

	// 抽象接口依赖（provider 无关）
	registrar   registry.Registrar
	discoverer  registry.Discoverer
	subscriber  registry.Subscriber
	configStore registry.ConfigStore

	depService map[string]*metadata.ServiceBasic

	// 已监听的服务集合（WatchService 去重）
	watchedServices map[string]bool
	// 配置变更回调（endpoint factory 注册 balancer 重置）
	configChangeCallbacks []func(serviceName string)

	// configGroup 是配置读取的分组（provider 无关的字符串，
	// 由门面从 provider 配置翻译注入，见 WithConfigGroup）
	configGroup string
	// selfServiceName 是本服务名，由 LocalServiceInit 记录，
	// Register/Deregister 用它构造自身实例（c.mu 保护）
	selfServiceName string

	// registeredInstances 缓存本 client 成功注册的实例身份（#75），
	// 键为 (服务名, 端口)。Deregister/DeregisterAs 优先用注册时的快照构造
	// 注销实例——注册与注销之间 GetPodIP 结果变化（网卡枚举顺序/POD_IP
	// 变更）、或调用方注销时漏传 WithGroup/WithCluster，都会让「现算的身份」
	// 与注册中心里的实例对不上，注销静默失效、实例残留（c.mu 保护）。
	registeredInstances map[instanceKey]registry.Instance

	// 服务实例变更回调：存指针包装（Go 函数不可比较，指针可按身份删除），
	// 并发订阅同一服务时单方失败只回滚自身、不抹掉他方已注册的回调
	instanceCallbacks map[string][]*instanceCallback
	// 已**成功**向 provider 注册 SDK 监听的服务集合（SubscribeService 去重，
	// 避免 N 次订阅 → 每次推送触发 N² 次回调）
	subscribedServices map[string]bool
	// subscribing 记录**正在注册** SDK 监听的服务。
	//
	// subscribedServices 只在注册**成功后**置位；注册期间用这个独立的
	// 「注册中」状态把两者分开。否则后到的并发订阅方会看到
	// subscribedServices 已置位 ⇒ 走去重路径直接返回 success；可前一个的
	// 注册随后**失败**并回滚 ⇒ 后一个以为订阅成功、实际没人注册监听 ⇒
	// 它的回调永远不会再被触发（永久失联）。
	//
	// 后到者改为等待在途注册的结果：成功则复用，失败则由它接手重试。
	subscribing map[string]*subscribeCall
	// unsubscribing 记录**正在注销** SDK 监听的服务（#147）。
	//
	// UnsubscribeService 删本地标记与 provider 侧退订不是原子的：若并发
	// SubscribeService 在「本地已清、provider.Unsubscribe 还在途」的窗口里
	// 完成新订阅，这次退订会把新监听一并拆除，而本地已标记为已订阅 ⇒
	// 永不重注册 ⇒ 实例变更推送全部丢失（服务路由静默僵死）。退订期间
	// ensureSubscribe 见到本标记时像对待在途注册一样等待其结束，窗口闭合。
	// 与 subscribing 复用同一结构（done 关闭即退订已出结果）。
	unsubscribing map[string]*subscribeCall
	callbackMu    sync.RWMutex

	// 生命周期：closed 是 atomic（#96——Close 持 c.mu 写、SubscribeService
	// 持 callbackMu 读，两把锁无 happens-before 边，bool 有数据竞争），
	// closeOnce 保证 Close 幂等。默认路径 SDK 客户端的所有权在 nns 门面
	// （门面创建、门面关闭），client 只清理自身监听状态
	closed    atomic.Bool
	closeOnce sync.Once

	// 本地实例缓存 - 解决网络不通时的容错
	instanceCache   map[string][]registry.Instance
	cacheMu         sync.RWMutex
	cacheExpiry     time.Duration
	cacheLastUpdate map[string]time.Time

	// singleflight 防止缓存击穿：并发请求只触发一次 Discover
	discoverSfg singleflight.Flight[[]registry.Instance]

	// 静态服务地址 - 本地开发或特殊场景直接配置
	staticInstances map[string][]registry.Instance
	// fallbackOnly 标记哪些服务的静态实例是「兜底」语义（WithStaticFallback），
	// 而非「短路」语义——见 WithStaticFallback 的说明
	fallbackOnly map[string]bool
	// fallbackOrder 全局降级序：静态实例与注册中心谁优先
	// （缺省 FallbackStaticFirst，见 WithFallbackOrder）
	fallbackOrder FallbackOrder

	// Mock 模式 - 本地开发不依赖 Nacos
	mockMode bool
	// 本地服务配置 - 不从 Nacos 获取
	localServiceConfig map[string]*metadata.ServiceBasic
}

// ClientOption ServiceClient 的配置选项函数类型。
type ClientOption func(*ServiceClient)

// WithCacheExpiry 设置实例缓存的过期时间。
//
// d <= 0（含零值缺省）时回退 defaultCacheExpiry（30s，与门面 nns.New 同缺省）：
// 零值不表示「禁用过期检查」——容错层没有「永不过期」语义，注册中心故障时
// 的新鲜缓存兜底（Discover 链第 3 级）依赖正常的过期判定（nns#94）。
func WithCacheExpiry(d time.Duration) ClientOption {
	return func(c *ServiceClient) { c.cacheExpiry = d }
}

// WithMockMode 设置是否启用 Mock 模式，Mock 模式下不连接 Nacos。
func WithMockMode(enabled bool) ClientOption {
	return func(c *ServiceClient) { c.mockMode = enabled }
}

// WithStaticInstances 配置指定服务的静态实例列表，用于本地开发或特殊场景。
//
// 默认是**短路**语义：配了该服务的静态实例后，GetInstances 直接返回它们，
// 不再查询注册中心。需要「注册中心优先、静态地址兜底」的生产形态请用
// WithStaticFallback（或全局 WithFallbackOrder(FallbackRegistryFirst)）。
//
// 同一服务上与 WithStaticFallback 重复配置时以最后一次调用为准——本选项把
// 该服务恢复为短路语义。
//
// 权重与注册侧同口径：Weight<=0（漏配或显式 0）回填默认权重 10，
// 显式正权重原样保留——见 normalizeStaticInstances。
func WithStaticInstances(serviceName string, instances []registry.Instance) ClientOption {
	return func(c *ServiceClient) {
		// 与运行期写入口共用锁内实现（#143）：归一化 + 清兜底标记 +
		// 发布全新切片（#149 读路径直接引用已发布快照）。构造期单线程，
		// 加锁仅为复用同一实现。
		c.mu.Lock()
		defer c.mu.Unlock()
		c.setStaticLocked(serviceName, instances, false, fallbackClear)
	}
}

// WithLocalServiceConfig 配置本地服务配置（不从 Nacos 获取）
// 用于本地开发或测试环境，直接指定服务的超时、重试等配置
func WithLocalServiceConfig(serviceName string, basic *metadata.ServiceBasic) ClientOption {
	return func(c *ServiceClient) {
		if c.localServiceConfig == nil {
			c.localServiceConfig = make(map[string]*metadata.ServiceBasic)
		}
		c.localServiceConfig[serviceName] = basic
	}
}

// WithRegistrar 注入自定义的服务注册器（用于测试或替换 provider）。
func WithRegistrar(r registry.Registrar) ClientOption {
	return func(c *ServiceClient) { c.registrar = r }
}

// WithDiscoverer 注入自定义的服务发现器（用于测试或替换 provider）。
func WithDiscoverer(d registry.Discoverer) ClientOption {
	return func(c *ServiceClient) { c.discoverer = d }
}

// WithSubscriber 注入自定义的服务订阅器（用于测试或替换 provider）。
func WithSubscriber(s registry.Subscriber) ClientOption {
	return func(c *ServiceClient) { c.subscriber = s }
}

// WithConfigStore 注入自定义的配置存储（用于测试或替换 provider）。
func WithConfigStore(s registry.ConfigStore) ClientOption {
	return func(c *ServiceClient) { c.configStore = s }
}

// WithConfigGroup 设置配置读取的分组（默认 provider 配置里的 group 概念，
// 本包只把它当不透明字符串透传给 ConfigStore）。
func WithConfigGroup(group string) ClientOption {
	return func(c *ServiceClient) { c.configGroup = group }
}

// getSelfServiceName 读取 LocalServiceInit 记录的自身服务名。
func (c *ServiceClient) getSelfServiceName() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.selfServiceName
}

// setSelfServiceNameIfEmpty 在尚未记录服务名时兜底设置
// （RunWithLifecycle 的 name 参数路径,Init 之前调用）。
func (c *ServiceClient) setSelfServiceNameIfEmpty(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.selfServiceName == "" {
		c.selfServiceName = name
	}
}

// instanceCallback 是实例变更回调的指针包装：slice 内按指针身份增删，
// 解决 Go 函数值不可比较导致的"整体快照回滚"在并发订阅下互相抹除的问题。
type instanceCallback struct {
	fn func([]registry.Instance)
}

// subscribeCall 是一次「向 provider 注册 SDK 监听」的进行中状态。
//
// done 关闭即表示该次注册**已有结果**（成功或失败）；等待者醒来后重新决策
// （读 subscribedServices / subscribing），而不是直接照抄结论。
type subscribeCall struct {
	done chan struct{}
}

// defaultCacheExpiry 是实例缓存过期时间的缺省值，与门面 nns.New 的缺省口径
// 一致（nns#94）：cacheExpiry 零值不表示「禁用过期」——time.Since 恒大于 0
// 会让 Discover 容错链第 3 级「新鲜缓存」写入即过期、每次直通注册中心，
// 故 New 对 <=0 的值（漏配或显式零/负值）统一回填缺省。
const defaultCacheExpiry = 30 * time.Second

// New 创建一个独立的 ServiceClient 实例（provider 无关）。
//
// 每次调用都返回全新实例，互不影响。本包不绑定任何注册中心 provider：
// 通过 WithRegistrar/WithDiscoverer/WithSubscriber/WithConfigStore 注入
// 实现（如 mock 或替代 provider）。未注入任何接口且未开 Mock 时保持
// 降级态（namingAvailable()==false，走缓存/静态实例路径）——Nacos 默认
// 构造由 nns 门面负责（门面是 provider 选择点），见 nns.New。
func New(opts ...ClientOption) *ServiceClient {
	c := &ServiceClient{
		depService:         make(map[string]*metadata.ServiceBasic),
		watchedServices:    make(map[string]bool),
		instanceCallbacks:  make(map[string][]*instanceCallback),
		subscribedServices: make(map[string]bool),
		subscribing:        make(map[string]*subscribeCall),
		unsubscribing:      make(map[string]*subscribeCall),
		instanceCache:      make(map[string][]registry.Instance),
		cacheLastUpdate:    make(map[string]time.Time),
		staticInstances:    make(map[string][]registry.Instance),
		fallbackOnly:       make(map[string]bool),
		localServiceConfig: make(map[string]*metadata.ServiceBasic),
	}

	for _, opt := range opts {
		opt(c)
	}

	// cacheExpiry 零值 = 漏配，回填缺省而不是让「新鲜缓存」容错层失效
	// （nns#94）：cacheExpiry==0 时 time.Since(lastUpdate) > 0 恒成立，
	// 缓存写入即过期，Discover 每次直通注册中心。
	if c.cacheExpiry <= 0 {
		c.cacheExpiry = defaultCacheExpiry
	}

	initMeta := make(map[string]string)
	c.localServiceMeta.Store(&initMeta)

	if c.mockMode {
		nnsLog.Info("running in mock mode, provider connection skipped")
		return c
	}

	if c.registrar == nil && c.discoverer == nil && c.subscriber == nil && c.configStore == nil {
		nnsLog.Warn("no provider injected and not mock; running degraded (cache/static instances only). " +
			"Use nns.New(cfg) for the nacos-backed default, or inject providers via With* options")
	}

	return c
}

// namingAvailable 检查命名服务是否可用（非 mock 且 discoverer 已就绪）。
func (c *ServiceClient) namingAvailable() bool {
	return !c.mockMode && c.discoverer != nil
}

// Close 释放 ServiceClient 持有的资源（幂等）。
// 停止所有配置监听、注销实例订阅、清空回调状态，最后关闭默认路径创建的
// Nacos SDK 客户端（各持 gRPC 长连接 + 后台 goroutine）。
// 保证 Close 后实例可安全重建（重新 WatchService/OnConfigChange 不残留旧状态）。
func (c *ServiceClient) Close() {
	// 兼容入口：用有界的默认 ctx（Close 不得无限期挂住）。
	_ = c.CloseWithContext(context.Background())
}

// CloseWithContext 关闭并返回错误（ctx 有界）。
//
// 与 Close 的差别只有两点：
//   - **ctx 有界**：Close 内部原先用 context.Background() 调 SDK 的
//     Unsubscribe / StopListenConfig ⇒ SDK 不响应时 Close **永久挂住**
//     （调用方无从得知卡在哪一步）。这里把 ctx 透传给调用方；Close 走
//     默认 5s 上限（见 closeDefaultTimeout）。
//   - **错误不吞**：注销订阅 / 停止配置监听的失败原样返回（Close 入口仍
//     只记日志——它没有返回值可给）。
//
// 幂等：与 Close 共享同一个 sync.Once，重复调用只有第一次真正执行。
func (c *ServiceClient) CloseWithContext(ctx context.Context) error {
	var errs []error
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed.Store(true)
		services := make([]string, 0, len(c.watchedServices))
		for svc := range c.watchedServices {
			services = append(services, svc)
		}
		c.watchedServices = make(map[string]bool)
		c.configChangeCallbacks = nil
		// Close 是终结操作：已注册实例的身份缓存一并清空（#75），
		// 复用需重新 New。
		c.registeredInstances = nil
		c.mu.Unlock()

		c.callbackMu.Lock()
		subscribed := make([]string, 0, len(c.subscribedServices))
		for svc := range c.subscribedServices {
			subscribed = append(subscribed, svc)
		}
		// 在途注册也要注销：注册方可能正在 provider 侧建立监听，
		// Close 时只清本地标记 ⇒ 监听残留（Close 后仍在推送）。
		for svc := range c.subscribing {
			if _, ok := c.subscribedServices[svc]; !ok {
				subscribed = append(subscribed, svc)
			}
		}
		c.subscribedServices = make(map[string]bool)
		c.instanceCallbacks = make(map[string][]*instanceCallback)
		c.callbackMu.Unlock()

		if ctx == nil {
			ctx = context.Background()
		}
		closeCtx, cancel := context.WithTimeout(ctx, closeDefaultTimeout)
		defer cancel()

		// 实例订阅注销：此前只停配置监听，SDK 侧订阅残留，
		// 推送回调依旧激活（Close 后仍改内部状态）
		if c.subscriber != nil && !c.mockMode {
			for _, svc := range subscribed {
				if err := c.subscriber.Unsubscribe(closeCtx, svc); err != nil {
					nnsLog.Warn("unsubscribe failed on close", "service", svc, "err", err)
					errs = append(errs, fmt.Errorf("unsubscribe %s: %w", svc, err))
				}
			}
		}
		if c.configStore != nil {
			for _, svc := range services {
				if err := c.configStore.StopListenConfig(closeCtx, svc, c.configGroup); err != nil {
					nnsLog.Warn("stop listen config failed", "service", svc, "err", err)
					errs = append(errs, fmt.Errorf("stop listen config %s: %w", svc, err))
				}
			}
		}
	})
	return errors.Join(errs...)
}

// closeDefaultTimeout 是 Close（无 ctx 入口）内部使用的默认上限。
//
// 关闭流程要调 SDK 的 Unsubscribe / StopListenConfig，这些调用在 SDK 不响应
// 时会一直阻塞 ⇒ Close 永久挂住、调用方无从得知卡在哪一步。给一个有限的上限，
// 宁可「关不干净但返回」也不要「永远挂着」。
const closeDefaultTimeout = 5 * time.Second

// MockMode 返回是否处于 Mock 模式。供 transport 适配器区分
// "mock 静默跳过"与"降级（registrar 不可用）"两种 registrar==nil 场景。
func (c *ServiceClient) MockMode() bool {
	return c.mockMode
}

// Registrar 返回底层注入的注册器；未注入时返回 nil。
//
// 逃生口：把「注册什么」交还给调用方（如按调用注入不同的实例字段），
// 而 SDK 连接与生命周期仍由门面持有与释放。与 Discoverer 配套。
func (c *ServiceClient) Registrar() registry.Registrar {
	return c.registrar
}

// Discoverer 返回底层注入的发现器；未注入时返回 nil。
//
// 逃生口：与 Registrar 配套，让调用方直接驱动发现路径
// （如按需携带分组/集群参数拉取实例），连接与生命周期仍归门面。
func (c *ServiceClient) Discoverer() registry.Discoverer {
	return c.discoverer
}

// GetBootstrap 已移除：它把具体注册中心配置类型暴露在公共 API 上，
// 与 ServiceClient 的 provider 无关设计矛盾。
