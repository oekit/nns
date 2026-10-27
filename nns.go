package nns

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oekit/logx"
	"github.com/oekit/nns/client"
	"github.com/oekit/nns/metadata"
	"github.com/oekit/registry"
	"github.com/oekit/registry/nacos"
	nacosimpl "github.com/oekit/registry/nacosimpl"
)

var nnsLog = logx.Delegating("nns")

// NNSClient NNS 服务客户端，封装了服务发现、负载均衡和端点管理功能。
//
// 门面是 provider 选择点：nns.New(cfg) 负责把 Nacos 配置构造为
// registry/nacos 实现并注入 provider 无关的 ServiceClient（2026-09-19
// 重构：默认构造从 nns/client 上提至此，治理核心不再绑定 provider）。
type NNSClient struct {
	client *client.ServiceClient

	// sdkClose 释放门面默认路径创建的 Nacos SDK 客户端
	// （naming/config client 各持 gRPC 长连接 + 后台 goroutine）。
	sdkClose  func()
	closeOnce sync.Once
}

// NNSOption NNSClient 的配置选项函数类型。
type NNSOption func(*clientOptions)

type clientOptions struct {
	cacheExpiry        time.Duration
	mockMode           bool
	staticInstances    map[string][]registry.Instance
	staticFallbackOnly map[string]bool
	fallbackOrder      client.FallbackOrder
	localServiceConfig map[string]*metadata.ServiceBasic
}

// WithCacheExpiry 设置实例缓存的过期时间。
func WithCacheExpiry(d time.Duration) NNSOption {
	return func(o *clientOptions) { o.cacheExpiry = d }
}

// WithMockMode 设置是否启用 Mock 模式，Mock 模式下不连接 Nacos。
func WithMockMode(enabled bool) NNSOption {
	return func(o *clientOptions) { o.mockMode = enabled }
}

// WithStaticInstance 配置指定服务的静态实例列表，用于本地开发或特殊场景。
//
// 默认是**短路**语义：配了该服务的静态实例后，GetInstances 直接返回它们，
// 不再查询注册中心。需要「注册中心优先、静态地址兜底」的生产形态请用
// WithStaticFallback（或全局 WithFallbackOrder(FallbackRegistryFirst)）。
func WithStaticInstance(serviceName string, instances []registry.Instance) NNSOption {
	return func(o *clientOptions) {
		if o.staticInstances == nil {
			o.staticInstances = make(map[string][]registry.Instance)
		}
		o.staticInstances[serviceName] = instances
		if o.staticFallbackOnly != nil {
			delete(o.staticFallbackOnly, serviceName)
		}
	}
}

// WithStaticFallback 配置指定服务的**兜底**静态实例。
//
// 与 WithStaticInstance 的区别只在参与时机：
//
//   - WithStaticInstance：短路静态——配了就直接返回，注册中心不被查询；
//   - WithStaticFallback：兜底静态——只有注册中心路径失败或返回空时才返回它。
//
// 典型用法是生产形态的「注册中心优先、静态地址兜底」：静态地址
// （如 k8s Service DNS）与注册中心同时配置，注册中心有健康实例时以
// 注册中心为准（静态地址在实例换 IP 后不会跟着变），注册中心冷启动或
// 故障时才用静态地址兜底。
//
// 同一服务上重复配置以最后一次调用为准。
func WithStaticFallback(serviceName string, instances []registry.Instance) NNSOption {
	return func(o *clientOptions) {
		if o.staticInstances == nil {
			o.staticInstances = make(map[string][]registry.Instance)
		}
		o.staticInstances[serviceName] = instances
		if o.staticFallbackOnly == nil {
			o.staticFallbackOnly = make(map[string]bool)
		}
		o.staticFallbackOnly[serviceName] = true
	}
}

// WithFallbackOrder 设置全局降级序：静态实例与注册中心谁优先。
//
// 缺省为 client.FallbackStaticFirst（静态实例短路注册中心，与既有行为
// 一致）。设为 client.FallbackRegistryFirst 后，全部服务的静态实例降级
// 为「兜底」：注册中心路径失败或返回空时才参与取值。
//
// 单服务可用 WithStaticFallback 单独指定兜底语义，不受全局序影响。
func WithFallbackOrder(order client.FallbackOrder) NNSOption {
	return func(o *clientOptions) { o.fallbackOrder = order }
}

// WithLocalServiceConfig 配置本地服务配置（不从 Nacos 获取）
// 用于本地开发或测试环境，直接指定服务的超时、重试等配置
func WithLocalServiceConfig(serviceName string, basic *metadata.ServiceBasic) NNSOption {
	return func(o *clientOptions) {
		if o.localServiceConfig == nil {
			o.localServiceConfig = make(map[string]*metadata.ServiceBasic)
		}
		o.localServiceConfig[serviceName] = basic
	}
}

// New 创建 NNS 客户端。
// cfg 是 Nacos 连接配置，opts 可选配置 mock 模式、静态实例、缓存过期等。
// 返回的 NNSClient 需调用 Init 初始化后使用。
//
// Provider 默认构造（非 mock 时）：创建 naming/config SDK 客户端并注入
// registry/nacos 的 Discoverer/Subscriber/Registrar/ConfigStore；构造失败
// 只 Warn 降级（缓存/静态实例路径），不阻断启动——与既有降级语义一致。
// 默认路径创建的 SDK 客户端归本实例所有，Close 时释放。
func New(cfg nacos.Bootstrap, opts ...NNSOption) *NNSClient {
	o := &clientOptions{
		cacheExpiry: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(o)
	}

	clientOpts := make([]client.ClientOption, 0, 8+len(o.staticInstances)+len(o.localServiceConfig))
	clientOpts = append(clientOpts,
		client.WithCacheExpiry(o.cacheExpiry),
		client.WithMockMode(o.mockMode),
		client.WithConfigGroup(cfg.Group),
		client.WithFallbackOrder(o.fallbackOrder),
	)

	for svc, instances := range o.staticInstances {
		if o.staticFallbackOnly[svc] {
			clientOpts = append(clientOpts, client.WithStaticFallback(svc, instances))
			continue
		}
		clientOpts = append(clientOpts, client.WithStaticInstances(svc, instances))
	}

	for svc, basic := range o.localServiceConfig {
		clientOpts = append(clientOpts, client.WithLocalServiceConfig(svc, basic))
	}

	var closers []func()
	if !o.mockMode {
		clientOpts, closers = appendNacosDefaults(clientOpts, cfg)
	}

	c := client.New(clientOpts...)

	nnsc := &NNSClient{
		client: c,
	}
	if len(closers) > 0 {
		nnsc.sdkClose = func() {
			for _, closeFn := range closers {
				closeFn()
			}
		}
	}
	return nnsc
}

// appendNacosDefaults 把 Nacos 配置物化为 provider 注入项。
// 构造函数理论上仅在 SDK client 为 nil 时失败（此处已排除），但失败模式
// 可能随版本变化，故记录日志而非静默吞掉——避免未来回归导致组件静默降级。
func appendNacosDefaults(clientOpts []client.ClientOption, cfg nacos.Bootstrap) ([]client.ClientOption, []func()) {
	namingClient, err := nacos.NewNamingClient(cfg)
	if err != nil {
		nnsLog.Warn("create naming client failed, will use cache/static instances", "err", err)
		return clientOpts, nil
	}

	if d, e := nacosimpl.NewDiscoverer(namingClient, nacosimpl.WithDiscovererGroup(cfg.Group)); e != nil {
		nnsLog.Warn("create discoverer failed, degrading to cache/static", "err", e)
	} else {
		clientOpts = append(clientOpts, client.WithDiscoverer(d))
	}
	if s, e := nacosimpl.NewSubscriber(namingClient, nacosimpl.WithSubscriberGroup(cfg.Group)); e != nil {
		nnsLog.Warn("create subscriber failed, degrading", "err", e)
	} else {
		clientOpts = append(clientOpts, client.WithSubscriber(s))
	}
	if r, e := nacosimpl.NewRegistrar(namingClient, nacosimpl.WithRegistrarGroup(cfg.Group)); e != nil {
		nnsLog.Warn("create registrar failed, degrading", "err", e)
	} else {
		clientOpts = append(clientOpts, client.WithRegistrar(r))
	}

	var closers []func()
	closers = append(closers, namingClient.CloseClient)

	configClient, err := nacos.NewConfigClient(cfg)
	if err != nil {
		nnsLog.Warn("create config client failed, will use local config", "err", err)
		return clientOpts, closers
	}
	if cs, e := nacosimpl.NewConfigStore(configClient); e != nil {
		nnsLog.Warn("create config store failed, degrading to local config", "err", e)
	} else {
		clientOpts = append(clientOpts, client.WithConfigStore(cs))
	}
	closers = append(closers, configClient.CloseClient)

	return clientOpts, closers
}

// Init 初始化本地服务并加载依赖服务配置，同时监听本地服务及所有依赖服务的配置变更（支持热更新）。
func (n *NNSClient) Init(ctx context.Context, serviceName string, depServices []string) error {
	_, err := n.client.LocalServiceInit(ctx, serviceName)
	if err != nil {
		return fmt.Errorf("init local service: %w", err)
	}

	if len(depServices) > 0 {
		if err := n.client.LoadDepService(ctx, depServices); err != nil {
			return fmt.Errorf("load dep services: %w", err)
		}
	}

	// 监听依赖服务配置变更（支持热更新）
	for _, ds := range depServices {
		n.client.WatchService(ctx, ds)
	}
	n.client.WatchService(ctx, serviceName)

	return nil
}

// Register 向注册中心注册当前服务实例。
func (n *NNSClient) Register(ctx context.Context, port int, opts ...registry.RegisterOptionFunc) error {
	return n.client.Register(ctx, port, opts...)
}

// RegisterAs 向注册中心注册当前服务实例到**指定服务名**下。
//
// 与 Register 的区别只有一处：服务名由调用方显式给出，而不是取 Init 记录的
// 自身服务名。因此 RegisterAs 不要求先 Init。
//
// 一个服务对外暴露多个端点（HTTP + RPC 双协议、分别注册在短名与完整 RPC
// 服务名下）时，用同一个 SDK 连接把每个名字各注册一次即可——不必为每个名字
// 各开一个门面实例（那会翻倍 Nacos gRPC 长连接与后台 goroutine）。
//
// 端口各自传入（同一进程的两个端点端口不同）。IP 取 GetPodIP 探测的本机 IP。
// opts 控制权重、元数据等注册选项。
//
// 配套的注销入口是 DeregisterAs：注册了几个名字就要注销几个名字，否则注册
// 中心里会留下无人回收的实例。
func (n *NNSClient) RegisterAs(ctx context.Context, serviceName string, port int, opts ...registry.RegisterOptionFunc) error {
	return n.client.RegisterAs(ctx, serviceName, port, opts...)
}

// Deregister 从注册中心注销当前服务实例。
func (n *NNSClient) Deregister(ctx context.Context, port int, opts ...registry.RegisterOptionFunc) error {
	return n.client.Deregister(ctx, port, opts...)
}

// DeregisterAs 从注册中心注销指定服务名下的当前服务实例。
//
// 与 RegisterAs 配套：注销的实例与服务名、端口必须与注册时一致，否则注册
// 中心匹配不到实例、注销会静默失效。
func (n *NNSClient) DeregisterAs(ctx context.Context, serviceName string, port int, opts ...registry.RegisterOptionFunc) error {
	return n.client.DeregisterAs(ctx, serviceName, port, opts...)
}

// Registrar 返回底层注入的注册器；未注入时返回 nil。
//
// 逃生口：把「注册什么」交还给调用方（如注册与本进程不同的实例地址），
// 而 SDK 连接与生命周期仍由本门面持有与释放。与 Discoverer 配套。
func (n *NNSClient) Registrar() registry.Registrar {
	return n.client.Registrar()
}

// Discoverer 返回底层注入的发现器；未注入时返回 nil。
//
// 逃生口：与 Registrar 配套，让调用方直接驱动发现路径，连接与生命周期
// 仍归本门面。
func (n *NNSClient) Discoverer() registry.Discoverer {
	return n.client.Discoverer()
}

// GetService 获取指定服务的基本配置信息。
func (n *NNSClient) GetService(ctx context.Context, serviceName string) (*metadata.ServiceBasic, error) {
	basic := n.client.ServiceWithName(ctx, serviceName)
	return &basic, nil
}

// GetInstances 获取指定服务的健康实例列表。
func (n *NNSClient) GetInstances(ctx context.Context, serviceName string) ([]registry.Instance, error) {
	return n.client.GetInstances(ctx, serviceName)
}

// GetAllInstances 获取指定服务的全部实例列表（包括不健康的实例）。
func (n *NNSClient) GetAllInstances(ctx context.Context, serviceName string) ([]registry.Instance, error) {
	return n.client.GetAllInstances(ctx, serviceName)
}

// Subscribe 订阅指定服务的实例变更，当实例列表变化时回调 callback。
func (n *NNSClient) Subscribe(ctx context.Context, serviceName string, callback func([]registry.Instance)) error {
	return n.client.SubscribeService(ctx, serviceName, callback)
}

// Unsubscribe 取消订阅指定服务的实例变更。
func (n *NNSClient) Unsubscribe(ctx context.Context, serviceName string) error {
	return n.client.UnsubscribeService(ctx, serviceName)
}

// AddStaticInstance 向指定服务追加静态实例地址。
func (n *NNSClient) AddStaticInstance(serviceName string, instances ...registry.Instance) {
	n.client.AddStaticInstance(serviceName, instances...)
}

// SetStaticInstances 设置指定服务的静态实例列表（覆盖原有）。
func (n *NNSClient) SetStaticInstances(serviceName string, instances []registry.Instance) {
	n.client.SetStaticInstances(serviceName, instances)
}

// Close 关闭 NNSClient 持有的所有资源（ServiceClient 的配置/实例订阅、
// 回调状态、门面默认路径创建的 Nacos SDK 客户端）。
// Close 是终结操作：默认路径下 SDK gRPC 连接已关闭，复用需重新 New。
// 端点工厂自 Round-4 起移至 ekit-nns-endpoint，其生命周期由构造方管理。
func (n *NNSClient) Close() {
	// 兼容入口：用有界的默认 ctx（Close 不得无限期挂住）。
	_ = n.CloseWithContext(context.Background())
}

// CloseWithContext 关闭并返回错误（ctx 有界）。
//
// 与 Close 的差别只有两点：
//   - **ctx 有界**：关闭流程要调 SDK 的注销订阅 / 停止配置监听，SDK 不响应时
//     Close 会**永久挂住**（调用方无从得知卡在哪一步）。这里把 ctx 透传给
//     调用方；Close 走默认 5s 上限。
//   - **错误不吞**：底层关闭失败原样返回（Close 入口仍只记日志——它没有返回
//     值可给）。
//
// 幂等：与 Close 共享同一个 sync.Once，重复调用只有第一次真正执行。
func (n *NNSClient) CloseWithContext(ctx context.Context) error {
	var errs []error
	n.closeOnce.Do(func() {
		if err := n.client.CloseWithContext(ctx); err != nil {
			errs = append(errs, err)
		}
		if n.sdkClose != nil {
			n.sdkClose()
		}
	})
	return errors.Join(errs...)
}

// ServiceClient 返回底层 *client.ServiceClient，供 nns/kitex、nns/hertz 等适配器使用。
//
// 推荐用法：
//
//	nnskitex.NewResolver(nnsc.ServiceClient())
//	nnshertz.NewRegistry(nnsc.ServiceClient())
//
// 不应绕过此访问器直接访问内部 ServiceClient 字段。
func (n *NNSClient) ServiceClient() *client.ServiceClient {
	return n.client
}
