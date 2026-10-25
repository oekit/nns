// Package nns 提供 Nacos Naming Service 高级封装，是 ekit 服务治理的官方推荐入口。
//
// 基于 registry 接口构建，提供 mock、静态实例、缓存、singleflight、多级降级等治理能力。
// 新服务应通过 NNSClient 接入，而非直接使用 registry/* 薄适配。
//
// # 核心特性
//
//   - 构造注入：每次 New 返回独立实例，推荐通过构造注入传递，不依赖全局状态
//   - 服务初始化：自动加载本地和依赖服务配置
//   - HTTP/gRPC Endpoint：自动生成服务端点
//   - 服务注册/注销：与 Nacos Naming 集成；RegisterAs 支持同一连接注册多个服务名
//   - 实例订阅：实时监听服务实例变更
//   - 多级降级：静态实例 > 缓存 > 注册中心（降级序可配置，支持注册中心优先 + 静态兜底）
//   - 框架适配：通过 ServiceClient() 暴露给 nns/kitex、nns/hertz 适配器
//
// # 快速接入
//
// 创建 NNSClient（推荐通过构造注入传递）：
//
//	ctx := context.Background()
//	nnsc := nns.New(cfg)
//	err := nnsc.Init(ctx, "my-service", []string{"user-service", "order-service"})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer nnsc.Close()
//
// 注册服务：
//
//	err = nnsc.Register(ctx, port, registry.WithMetadata(version.Metadata()))
//
// 获取服务实例：
//
//	instances, err := nnsc.GetInstances(ctx, "user-service")
//
// 创建 HTTP Endpoint（nns-endpoint 模块，工厂由调用方持有；nns 包自拆分起
// 不再内置 MakeHTTPEndpoint——此前包注释仍展示已移除方法，照抄会编译失败，
// oekit/oekit.github.io#12）：
//
//	import "github.com/oekit/nns-endpoint/endpoint"
//
//	factory := endpoint.NewHTTPEndpointFactory(nnsc.ServiceClient())
//	ep := factory("user-service")
//
// 框架适配：
//
//	// Kitex
//	cli, _ := echo.NewClient("dep",
//	    client.WithResolver(nnskitex.NewResolver(nnsc.ServiceClient())))
//	svr, _ := echo.NewServer(handler,
//	    server.WithRegistry(nnskitex.NewRegistry(nnsc.ServiceClient())))
//
//	// Hertz
//	cli.Use(sd.Discovery(nnshertz.NewResolver(nnsc.ServiceClient())))
//	svr := server.New(..., server.WithRegistry(nnshertz.NewRegistry(nnsc.ServiceClient())))
//
// # 配置选项
//
//   - WithCacheExpiry: 缓存过期时间，默认 30s
//   - WithMockMode: 启用模拟模式（本地开发）
//   - WithStaticInstance: 静态实例配置（短路语义）
//   - WithStaticFallback: 静态兜底实例（注册中心优先，注册中心无果时参与）
//   - WithFallbackOrder: 全局降级序（默认 client.FallbackStaticFirst）
//   - WithLocalServiceConfig: 本地服务配置（不从 Nacos 获取）
//
// # 多端点服务
//
// 一个服务对外暴露多个端点（HTTP + RPC 双协议）时，用 RegisterAs 把每个名字
// 各注册一次即可——同一个 SDK 连接，不必为每个名字各开一个门面实例：
//
//	err = nnsc.RegisterAs(ctx, "shop-http", 8080)
//	err = nnsc.RegisterAs(ctx, "com.example.shop.order", 9090)
//	// 关停时对称注销
//	err = nnsc.DeregisterAs(ctx, "shop-http", 8080)
//	err = nnsc.DeregisterAs(ctx, "com.example.shop.order", 9090)
//
// # 与 bootstrap 集成
//
// 使用 bootstrap.Run 并设置 DepServices 自动初始化 NNS：
//
//	b, _ := bootstrap.New(bootstrap.WithServiceName("my-service"),
//	    bootstrapnacos.WithNNS(nacosCfg, port, []string{"dep.service"})) // bootstrap/nacos
//	b.Run(func(app *bootstrap.App) error {
//	    // ...
//	})
package nns
