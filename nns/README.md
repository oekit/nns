# NNS - Nacos Naming Service

基于 Nacos 的服务发现和服务治理门面，是 ekit 服务治理的**官方推荐入口**。

> `registry/*` 是底层 SPI 和框架薄适配，不含缓存/降级/治理能力。新服务应通过 NNS 接入。

## 核心特性

- 服务注册与发现
- 负载均衡（RoundRobin、Random、WeightedRandom）
- HTTP/gRPC 远程调用
- 熔断器保护
- **本地缓存** - Nacos 不可用时自动降级
- **静态地址** - 本地开发直接配置服务地址
- **本地配置** - 不依赖 Nacos 配置中心

## 快速开始

### 生产环境

```go
import (
    "context"
    "github.com/yi-nology/ekit-nns/nns"
)

cfg := nacos.Bootstrap{
    Addr:        "nacos.enjoydream.svc.cluster.local:8848",
    NamespaceId: "dev",
    Group:       "DEFAULT_GROUP",
}

ctx := context.Background()
client := nns.New(cfg)
err := client.Init(ctx, "my-service", []string{"user-service"})
```

### 本地开发

本地开发通常有两个问题：
1. 没有 Nacos 环境
2. 不知道下游服务地址

解决方案：

```go
cfg := nacos.Bootstrap{}

ctx := context.Background()
client := nns.New(cfg,
    // 1. 开启 Mock 模式（不连接 Nacos）
    nns.WithMockMode(true),

    // 2. 配置本地服务配置（超时、重试等）
    nns.WithLocalServiceConfig("user-service", &metadata.ServiceBasic{
        ConnTimeout: 1000,
        ReadTimeout: 5000,
        Retry:       3,
        Lb:          "roundrobin",
    }),

    // 3. 配置静态服务地址
    nns.WithStaticInstance("user-service", []metadata.Instance{
        {IP: "192.168.1.100", Port: 8080, Weight: 10},
    }),
)

err := client.Init(ctx, "my-service", []string{"user-service"})
```

## 配置说明

### WithMockMode

开启 Mock 模式，完全不连接 Nacos。适用于：
- 本地开发
- 单元测试
- 网络隔离环境

```go
nns.WithMockMode(true)
```

### WithStaticInstance

配置静态服务地址，不通过服务发现获取。优先级最高。

```go
// 初始化时配置
nns.WithStaticInstance("user-service", []metadata.Instance{
    {IP: "192.168.1.100", Port: 8080},
    {IP: "192.168.1.101", Port: 8080},
})

// 运行时动态添加
client.AddStaticInstance("user-service", metadata.Instance{
    IP: "192.168.1.100", Port: 8080,
})
```

### WithLocalServiceConfig

配置本地服务配置，不从 Nacos 配置中心获取。用于：
- 本地开发没有配置中心
- 测试环境需要特殊配置
- 覆盖远程配置

```go
nns.WithLocalServiceConfig("user-service", &metadata.ServiceBasic{
    ConnTimeout: 1000,    // 连接超时 1s
    ReadTimeout: 5000,    // 读取超时 5s
    WriteTimeout: 5000,   // 写入超时 5s
    Retry:       3,       // 重试 3 次
    Lb:          "roundrobin",  // 负载均衡策略
})
```

### WithCacheExpiry

配置本地缓存过期时间。Nacos 获取的实例会缓存到本地，当 Nacos 不可用时使用缓存。

```go
nns.WithCacheExpiry(60 * time.Second)  // 缓存 60 秒
```

## 实例获取优先级

```
获取服务实例
    ↓
[有静态地址?] ──是──→ 返回静态地址
    ↓ 否
[Nacos 可用?] ──是──→ 从 Nacos 获取并缓存
    ↓ 否
[缓存有效?] ──是──→ 返回缓存实例
    ↓ 否
返回错误
```

## 使用示例

### HTTP 调用

```go
endpoint, err := client.MakeHTTPEndpoint("user-service")
if err != nil { ... }
err = endpoint.Call(ctx, "GET", "/api/users/1", nil, &result,
    endpoint.WithTimeout(5*time.Second),
    endpoint.WithHeader("X-Request-ID", "123"),
)
```

### gRPC 调用

```go
endpoint, err := client.MakeGRPCEndpoint("order-service")
if err != nil { ... }
err = endpoint.Call(ctx, "OrderService", "GetOrder", req, &reply,
    endpoint.WithTimeout(3*time.Second),
)
```

### 服务订阅

```go
err := client.Subscribe(ctx, "user-service", func(instances []metadata.Instance) {
    for _, inst := range instances {
        fmt.Printf("Instance: %s:%d\n", inst.IP, inst.Port)
    }
})
```

### 框架适配：Kitex

```go
import nnskitex "github.com/yi-nology/ekit-kitex/nns/kitex"

// Client with NNS discovery
cli, _ := echo.NewClient("dep-service",
    client.WithResolver(nnskitex.NewResolver(client.ServiceClient())),
)
// Server with NNS registration
svr, _ := echo.NewServer(handler,
    server.WithRegistry(nnskitex.NewRegistry(client.ServiceClient())),
)
```

### 框架适配：Hertz

```go
import nnshertz "github.com/yi-nology/ekit-hertz/nns/hertz"
import sd "github.com/cloudwego/hertz/pkg/app/client/service_discovery"

cli.Use(sd.Discovery(nnshertz.NewResolver(client.ServiceClient())))
svr := server.New(server.WithHostPorts(":8080"),
    server.WithRegistry(nnshertz.NewRegistry(client.ServiceClient())),
)
```

## 场景示例

### 场景 1：本地开发，连接远程 Nacos

```go
cfg := nacos.Bootstrap{
    Addr:        "nacos.test.com:8848",
    NamespaceId: "dev",
    Group:       "DEFAULT_GROUP",
}

ctx := context.Background()
client := nns.New(cfg)
client.Init(ctx, "my-service", []string{"user-service"})
```

### 场景 2：本地开发，完全离线

```go
cfg := nacos.Bootstrap{}

ctx := context.Background()
client := nns.New(cfg,
    nns.WithMockMode(true),
    nns.WithStaticInstance("user-service", []metadata.Instance{
        {IP: "localhost", Port: 8080},
    }),
)
client.Init(ctx, "my-service", []string{"user-service"})
```

### 场景 3：测试环境，自定义配置

```go
cfg := nacos.Bootstrap{
    Addr:        "nacos.test.com:8848",
    NamespaceId: "test",
}

ctx := context.Background()
client := nns.New(cfg,
    // 覆盖远程配置
    nns.WithLocalServiceConfig("user-service", &metadata.ServiceBasic{
        ReadTimeout: 10000,  // 测试环境超时设长
        Retry:       5,
    }),
    // 备用地址（Nacos 挂了还能用）
    nns.WithStaticInstance("user-service", []metadata.Instance{
        {IP: "192.168.1.100", Port: 8080},
    }),
)
client.Init(ctx, "my-service", []string{"user-service"})
```

### 场景 4：K8s 环境，使用环境变量

```go
import "os"

cfg := nacos.Bootstrap{
    Addr:        os.Getenv("NACOS_ADDR"),        // nacos.enjoydream.svc.cluster.local:8848
    NamespaceId: os.Getenv("NACOS_NAMESPACE"),    // prod
    Group:       os.Getenv("NACOS_GROUP"),        // DEFAULT_GROUP
}

ctx := context.Background()
client := nns.New(cfg)
client.Init(ctx, "my-service", []string{"user-service"})
```
