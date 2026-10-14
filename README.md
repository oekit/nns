# ekit-nns

> NNS 服务治理客户端——provider 无关的服务发现/配置/实例缓存/订阅，含端点级熔断与换实例重试。

本仓库是 [ekit](https://git.enjoye.top/enjoydream/ekit)（`git.enjoye.top/enjoydream/ekit` v0.34.0）
按框架耦合分层拆分出的独立 Go module，属于 **yi-nology** ekit 子项目矩阵。

## 安装

```bash
go get github.com/yi-nology/ekit-nns@v0.1.0
```

## 包清单

- `nns/` — 门面：`nns.New(Bootstrap)` 一行创建（Nacos 默认构造可注入替换），mock 模式、Close 终结化
- `nns/client/` — ServiceClient 核心：服务发现、配置、实例缓存 + singleflight、多级降级
- `nns/endpoint/` — 端点工厂：per-service 熔断（4xx 不计失败）、HTTP 换实例重试、弹性指标
- `nns/metadata/` — 实例元数据

## 依赖关系

子项目矩阵与依赖方向（自底向上）：

```
ekit-core                      纯工具层（零内部依赖）
  └─ ekit-observability        日志/追踪/指标门面
       └─ ekit-registry        注册发现 + 配置
            ├─ ekit-data       存储层
            ├─ ekit-network    网络客户端
            │    └─ ekit-nns   服务治理客户端
            │         └─ ekit-bootstrap  应用引导
            ├─ ekit-hertz      Hertz 生态
            ├─ ekit-kitex      Kitex 生态
            └─ ekit-workflow   流程编排
```

依赖：[ekit-core](https://github.com/yi-nology/ekit-core)、[ekit-observability](https://github.com/yi-nology/ekit-observability)、[ekit-registry](https://github.com/yi-nology/ekit-registry)、[ekit-network](https://github.com/yi-nology/ekit-network)。Kitex/Hertz 适配器分属 ekit-kitex / ekit-hertz。

## 与 ekit 单体仓库的关系

- 代码与 ekit v0.34.0 等价，仅 module 路径从 `git.enjoye.top/enjoydream/ekit/...` 改为 `github.com/yi-nology/ekit-nns/...`
- 后续演进以各子仓库独立发版（SemVer）；ekit 单体仓库保留完整历史与架构文档（docs/PLATFORM_CONTRACT.md）
- 开发规范（命名/错误处理/并发安全/日志门面）沿用 ekit AGENTS.md 约定

## License

见 [LICENSE](LICENSE)
