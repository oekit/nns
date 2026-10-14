# ekit-nns

> NNS 服务治理客户端——provider 无关的服务发现/配置/实例缓存/订阅，端点级熔断与换实例重试。

本仓库是 [ekit](https://git.enjoye.top/enjoydream/ekit)（`git.enjoye.top/enjoydream/ekit` v0.34.0）
拆分出的独立 Go module，属于 **yi-nology** ekit 子项目矩阵（Round-2 深拆粒度）。

## 安装

```bash
go get github.com/yi-nology/ekit-nns@v0.2.0
```

## 包清单

- \`nns/\` — 门面：nns.New 一行创建、mock 模式、Close 终结化
- \`nns/client/\` — ServiceClient 核心
- \`nns/endpoint/\` — 端点工厂：per-service 熔断（4xx 不计失败）、HTTP 换实例重试、弹性指标
- \`nns/metadata/\` — 实例元数据

## 依赖关系

子项目矩阵与依赖方向（自底向上，19 仓）：

```
ekit-core            文本/数据工具 + governance/lifecycle/version      （零内部依赖）
ekit-concurrency     并发与流控原语                                     （零内部依赖）
ekit-logx            日志门面 + shutdown                                （零内部依赖）
ekit-istio           Istio Proxyless                                    （零内部依赖）
  ekit-metrics       Prometheus 门面                          → core
  ekit-tracing       OTel + traceid                           → logx
  ekit-registry      注册发现 + 配置                          → core, logx
    ekit-data        缓存抽象 + 定时任务                      → core, logx, concurrency
      ekit-gorm      SQL/GORM 全家                            → core, logx
      ekit-redis     Redis 全家                               → data, core, logx
    ekit-httpx       HTTP 客户端 + 负载均衡                   → core, concurrency, logx, metrics, tracing, registry
      ekit-nns       服务治理客户端                           → httpx, registry, core, logx, tracing, metrics
        ekit-bootstrap 应用引导
    ekit-middleware  Hertz 中间件栈                           → core, concurrency, logx, metrics, tracing, registry
    ekit-hertz       Hertz 运行时/适配/instrumentation        → middleware, gorm, nns, registry, metrics, tracing, logx, core
    ekit-kitex       Kitex 生态                               → nns, registry, core, concurrency, logx, metrics
  ekit-workflow      流程编排                                 → concurrency, logx
```

> 注：`ekit-observability` 已在 Round-2 深拆中解散——logx 迁 [ekit-logx]、metrics 迁 [ekit-metrics]、
> tracing/traceid 迁 [ekit-tracing]、shutdown 迁 [ekit-logx]；原仓库已归档。

依赖：[ekit-httpx](https://github.com/yi-nology/ekit-httpx)、[ekit-registry](https://github.com/yi-nology/ekit-registry)、[ekit-core](https://github.com/yi-nology/ekit-core)、[ekit-logx](https://github.com/yi-nology/ekit-logx)、[ekit-tracing](https://github.com/yi-nology/ekit-tracing)、[ekit-metrics](https://github.com/yi-nology/ekit-metrics)、[ekit-concurrency](https://github.com/yi-nology/ekit-concurrency)。

## 与 ekit 单体仓库的关系

- 代码与 ekit v0.34.0 等价，module 路径 `git.enjoye.top/enjoydream/ekit/...` → `github.com/yi-nology/ekit-nns/...`
- 各子仓库独立发版（SemVer）；开发规范沿用 ekit AGENTS.md 约定

## License

见 [LICENSE](LICENSE)
