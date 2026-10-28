## [v0.5.3] — 2026-10-28

- fix(client): `SubscribeService` 的 check-then-act 竞态——`subscribedServices` 在调用 provider **之前**就置位 ⇒ 后到的并发订阅方直接返回 success，可前一个注册随后失败并回滚 ⇒ 后一个以为订阅成功、实际没人注册监听 ⇒ 回调永远不再被触发（永久失联）。修法：拆出「注册中」状态（`subscribing`），后到者等待在途注册的结果，失败则接手重试；多个等待者同时醒来时只有一个能拿到注册资格 ⇒ provider 只被注册一次

## [v0.5.2] — 2026-10-28

- fix(metadata): `GetTimeoutRetryConfig(protocol, serviceName, api)` 的文档写「根据协议、服务名和接口名获取超时重试配置」，但规则项 `TimeoutRetry` 只有 `Api`/`Type` 两个维度、**没有协议与服务名字段** ⇒ protocol 与 serviceName 两个参数被**静默忽略**。照文档理解会以为「换个协议查到的规则不同」，实际拿到的是同一条规则。文档改为写明真实匹配口径（只有 api 参与匹配）并说明两个参数为何保留；加用例把该口径钉成可执行的事实（文档若改成参与匹配，用例当场红）

## [v0.5.1] — 2026-10-28

- fix(nns): 关闭流程的 ctx **无界** ⇒ SDK 不响应时 `Close()` 永久挂住（关闭流程要调 SDK 的 `Unsubscribe`/`StopListenConfig`，此前内部用 `context.Background()`，调用方既拿不到返回值也无从得知卡在哪一步）；且关闭错误只记日志、调用方拿不到任何错误。新增 `CloseWithContext(ctx) error`（`NNSClient` 与 `client.ServiceClient` 各一个）：ctx 透传给调用方、`Close()` 入口走默认 5s 上限、关闭错误原样返回。**`Close()` 签名与幂等语义不变**（仍是无参、共享同一个 `sync.Once`），消费方不需要改调用点

## [v0.5.0] — 2026-10-25

- feat(client): 多端点服务名注册——RegisterAs / DeregisterAs 让同一 SDK 连接注册多个服务名（HTTP + RPC 双端点），新增 Registrar() / Discoverer() 逃生口（#30）
- feat(client): 注册中心优先、静态地址兜底——新增 FallbackOrder / WithFallbackOrder / WithStaticFallback，GetInstances 降级链扩到六级；既有 WithStaticInstances 短路语义不变，既有调用方零行为变化（#31）

## [v0.4.4] — 2026-10-21

- 改用嵌套模块 registry/nacos 与 registry/nacosimpl——registry v0.5.0 起不再提供这两个子包（import 路径不变，由独立 module 提供）；registry 地板 v0.4.1 → v0.5.0

## [v0.4.3] — 2026-10-19

- build: 矩阵内依赖地板对齐（align_floors 不动点）：concurrency v0.5.4 → v0.17.0、core v0.7.0 → v0.9.2、logx v0.6.0 → v0.7.0

## [v0.4.2] — 2026-10-18

### docs：包 overview 移除已删除的 MakeHTTPEndpoint 示例（[oekit/oekit.github.io#12](https://github.com/oekit/oekit.github.io/issues/12)）
- doc.go「创建 HTTP Endpoint」段仍展示拆分时已移除的 `nnsc.MakeHTTPEndpoint`——
  pkg.go.dev 包首页照抄会编译失败。改为现行形态：nns-endpoint 模块
  `endpoint.NewHTTPEndpointFactory(nnsc.ServiceClient())`（工厂由调用方持有），
  并注明背景。纯文档零代码改动

# Changelog — oekit/nns

由 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来。所有 notable 变更记录如下。

## [v0.4.1] — 2026-10-18

- fix(deps): logx 地板 v0.5.0 → v0.6.0、core 地板 v0.6.0 → v0.7.0、registry 地板 v0.4.0 → v0.4.1（零代码改动）

## [v0.4.0] — 2026-10-17

### Breaking
- **包提升至模块根**：
  - `github.com/oekit/nns/nns` → `github.com/oekit/nns`
  - `github.com/oekit/nns/nns/client` → `github.com/oekit/nns/client`
  - `github.com/oekit/nns/nns/metadata` → `github.com/oekit/nns/metadata`
  仅移动目录与导入路径，代码零逻辑变化
- 依赖升级：registry v0.4.0（SPI 包提升至模块根）

## [v0.3.5] — 2026-10-17

- concurrency 地板 v0.5.2 → v0.5.4

## [v0.3.4] — 2026-10-17

- fix(deps): 内部依赖地板全量对齐最新——concurrency v0.5.2、core v0.6.0、logx v0.5.0、registry v0.3.1；go 最低版本降至 1.24.0——prometheus 家族（client_golang v1.23.2/procfs v0.19.2）、x/crypto v0.48、x/net v0.50、x/sync v0.19、x/sys v0.41、x/text v0.34、grpc v1.80、genproto/rpc 20260120、httpsnoop v1.0.4 锁 go1.24 兼容版

## [v0.1.0] — 2026-10-15

自 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来的初始公开版本，完成拆分稳定化：模块路径统一为 `github.com/oekit/nns`，多轮拆分/合并收敛至当前模块边界，组织基建（共享 CI workflow、golangci v2、Dependabot）就位。