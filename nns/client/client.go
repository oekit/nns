package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yi-nology/ekit-core/concurrency/singleflight"
	"github.com/yi-nology/ekit-nns/nns/metadata"
	"github.com/yi-nology/ekit-observability/observability/logx"
	"github.com/yi-nology/ekit-registry/registry"
)

var nnsLog = logx.Delegating("nns")

// ErrRegistrarUnavailable 在 ServiceClient.Register 被调用但未配置 registrar 时返回。
// 常见场景：未注入 registrar、Mock 模式未启用、provider 构造失败降级。
var ErrRegistrarUnavailable = errors.New("nns: registrar not available")

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

	// 服务实例变更回调：存指针包装（Go 函数不可比较，指针可按身份删除），
	// 并发订阅同一服务时单方失败只回滚自身、不抹掉他方已注册的回调
	instanceCallbacks map[string][]*instanceCallback
	// 已向 provider 注册 SDK 监听的服务集合（SubscribeService 去重，
	// 避免 N 次订阅 → 每次推送触发 N² 次回调）
	subscribedServices map[string]bool
	callbackMu         sync.RWMutex

	// 生命周期：closed 由 c.mu 保护（WatchService 竞态检查用），
	// closeOnce 保证 Close 幂等。默认路径 SDK 客户端的所有权在 nns 门面
	// （门面创建、门面关闭），client 只清理自身监听状态
	closed    bool
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

	// Mock 模式 - 本地开发不依赖 Nacos
	mockMode bool
	// 本地服务配置 - 不从 Nacos 获取
	localServiceConfig map[string]*metadata.ServiceBasic
}

// ClientOption ServiceClient 的配置选项函数类型。
type ClientOption func(*ServiceClient)

// WithCacheExpiry 设置实例缓存的过期时间。
func WithCacheExpiry(d time.Duration) ClientOption {
	return func(c *ServiceClient) { c.cacheExpiry = d }
}

// WithMockMode 设置是否启用 Mock 模式，Mock 模式下不连接 Nacos。
func WithMockMode(enabled bool) ClientOption {
	return func(c *ServiceClient) { c.mockMode = enabled }
}

// WithStaticInstances 配置指定服务的静态实例列表，用于本地开发或特殊场景。
func WithStaticInstances(serviceName string, instances []registry.Instance) ClientOption {
	return func(c *ServiceClient) {
		if c.staticInstances == nil {
			c.staticInstances = make(map[string][]registry.Instance)
		}
		c.staticInstances[serviceName] = instances
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
		instanceCache:      make(map[string][]registry.Instance),
		cacheLastUpdate:    make(map[string]time.Time),
		staticInstances:    make(map[string][]registry.Instance),
		localServiceConfig: make(map[string]*metadata.ServiceBasic),
	}

	for _, opt := range opts {
		opt(c)
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
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		services := make([]string, 0, len(c.watchedServices))
		for svc := range c.watchedServices {
			services = append(services, svc)
		}
		c.watchedServices = make(map[string]bool)
		c.configChangeCallbacks = nil
		c.mu.Unlock()

		c.callbackMu.Lock()
		subscribed := make([]string, 0, len(c.subscribedServices))
		for svc := range c.subscribedServices {
			subscribed = append(subscribed, svc)
		}
		c.subscribedServices = make(map[string]bool)
		c.instanceCallbacks = make(map[string][]*instanceCallback)
		c.callbackMu.Unlock()

		ctx := context.Background()
		// 实例订阅注销：此前只停配置监听，SDK 侧订阅残留，
		// 推送回调依旧激活（Close 后仍改内部状态）
		if c.subscriber != nil && !c.mockMode {
			for _, svc := range subscribed {
				if err := c.subscriber.Unsubscribe(ctx, svc); err != nil {
					nnsLog.Warn("unsubscribe failed on close", "service", svc, "err", err)
				}
			}
		}
		if c.configStore != nil {
			for _, svc := range services {
				if err := c.configStore.StopListenConfig(ctx, svc, c.configGroup); err != nil {
					nnsLog.Warn("stop listen config failed", "service", svc, "err", err)
				}
			}
		}
	})
}

// MockMode 返回是否处于 Mock 模式。供 transport 适配器区分
// "mock 静默跳过"与"降级（registrar 不可用）"两种 registrar==nil 场景。
func (c *ServiceClient) MockMode() bool {
	return c.mockMode
}

// GetBootstrap 已移除：它把具体注册中心配置类型暴露在公共 API 上，
// 与 ServiceClient 的 provider 无关设计矛盾。
