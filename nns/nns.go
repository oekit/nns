package nns

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/yi-nology/ekit-registry/nacos"
	"github.com/yi-nology/ekit-nns/nns/client"
	"github.com/yi-nology/ekit-nns/nns/endpoint"
	"github.com/yi-nology/ekit-nns/nns/metadata"
	"github.com/yi-nology/ekit-observability/observability/logx"
	"github.com/yi-nology/ekit-registry/registry"
	nacosimpl "github.com/yi-nology/ekit-registry/registry/nacos"
)

var nnsLog = logx.Delegating("nns")

// NNSClient NNS 服务客户端，封装了服务发现、负载均衡和端点管理功能。
//
// 门面是 provider 选择点：nns.New(cfg) 负责把 Nacos 配置构造为
// registry/nacos 实现并注入 provider 无关的 ServiceClient（2026-09-19
// 重构：默认构造从 nns/client 上提至此，治理核心不再绑定 provider）。
type NNSClient struct {
	client      *client.ServiceClient
	httpFactory *endpoint.HTTPEndpointFactory
	grpcFactory *endpoint.GRPCEndpointFactory

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
func WithStaticInstance(serviceName string, instances []registry.Instance) NNSOption {
	return func(o *clientOptions) {
		if o.staticInstances == nil {
			o.staticInstances = make(map[string][]registry.Instance)
		}
		o.staticInstances[serviceName] = instances
	}
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
	)

	for svc, instances := range o.staticInstances {
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

	httpFactory := endpoint.NewHTTPEndpointFactory(c)
	grpcFactory := endpoint.NewGRPCEndpointFactory(c)

	nnsc := &NNSClient{
		client:      c,
		httpFactory: httpFactory,
		grpcFactory: grpcFactory,
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

// Deregister 从注册中心注销当前服务实例。
func (n *NNSClient) Deregister(ctx context.Context, port int, opts ...registry.RegisterOptionFunc) error {
	return n.client.Deregister(ctx, port, opts...)
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

// MakeHTTPEndpoint 为指定服务创建 HTTP 端点。
func (n *NNSClient) MakeHTTPEndpoint(serviceName string) (endpoint.Endpoint, error) {
	return n.httpFactory.MakeEndpoint(serviceName)
}

// MakeGRPCEndpoint 为指定服务创建 gRPC 端点。
func (n *NNSClient) MakeGRPCEndpoint(serviceName string) (endpoint.Endpoint, error) {
	return n.grpcFactory.MakeEndpoint(serviceName)
}

// Close 关闭 NNSClient 持有的所有资源（ServiceClient 的配置/实例订阅、
// 回调状态、门面默认路径创建的 Nacos SDK 客户端、端点工厂）。
// Close 是终结操作：默认路径下 SDK gRPC 连接已关闭，复用需重新 New。
func (n *NNSClient) Close() {
	n.closeOnce.Do(func() {
		n.client.Close()
		if n.sdkClose != nil {
			n.sdkClose()
		}
		if n.httpFactory != nil {
			n.httpFactory.Close()
		}
		if n.grpcFactory != nil {
			n.grpcFactory.Close()
		}
	})
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
