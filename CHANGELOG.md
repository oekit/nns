## [v0.5.16] — 2026-11-10

- - build: 依赖地板对齐（floor sync，无代码变更）

## [v0.5.15] — 2026-11-10

- - build: 依赖地板对齐（floor sync，无代码变更）

## [v0.5.14] — 2026-11-10

- - fix(client): ensureSubscribe 等待在途注册时不再提前摘除自身回调条目——并发订阅同一服务后到者回调可正常触发，ctx 取消才回滚（#104）

关联问题单：nns#104。

## [v0.5.13] — 2026-11-08

### fix(client): 订阅等待者 ctx 取消回滚自身条目（#95）+ closed 原子化（#96）

#95：等待在途注册时 ctx 超时返回，回调条目按指针身份摘除（泄漏实测复现，回归钉独立通道断言）；
#96：closed bool 改 atomic.Bool（Close 持 c.mu 写、SubscribeService 持 callbackMu 读，两把锁无 happens-before）。

## [v0.5.12] — 2026-11-09

- fix(client): client.New() 零值 cacheExpiry 回填 30s 缺省——容错层恢复生效（#94）

关联问题单：nns#94。

## [v0.5.11] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [v0.5.10] — 2026-11-07

- fix(client): Register/RegisterAs 成功后缓存实例快照——Deregister 不再重新推导身份，IP/Group 漂移不再静默注销失败（#75）
- docs(client): RunWithLifecycle 调用顺序契约修正——server 先注册再 RunWithLifecycle，关停序「先反注册后关 server」兑现（#76）
- fix(nns): GetService 返回真实错误 + 新增 ServiceConfig（错误不吞、ErrConfigStoreUnavailable 哨兵）；Required 注册失败中止启动的部分在 bootstrap/nacos 侧落地（PR #106）（#77）

关联问题单：nns#75、nns#76、nns#77。

## [v0.5.9] — 2026-11-06

- **BREAKING** fix(client): mock/未启用注册发现时 Register/Deregister 改为幂等 no-op success（[#66](https://github.com/oekit/nns/issues/66)）——此前返回 ErrRegistrarUnavailable，消费方无法区分「真故障」与「本来就没注册器」，被迫自维护 `Enabled && !MockMode` 门控布尔并保证注册/注销对称（每个用 nns 的服务重复这套判断；octopus main.go 的 nnsSelfRegister 布尔即此形态）。修法（issue 方案 2）：没注册器 = 幂等 no-op success（与 `nns.enabled: false` 的「不注册」语义对齐，mock 开箱即用）；真故障（Nacos 连不上）走 registrar 内部 error 路径仍可区分。**升级可见**：依赖 ErrRegistrarUnavailable 判定的消费方需改为直接调用（no-op 已成功）；哨兵保留供兼容
- fix(client): 注册路径拒绝回环 Pod IP（[#68](https://github.com/oekit/nns/issues/68)）——`GetPodIP` 的 127.0.0.1 静默回退（网卡枚举失败）被注册链直接消费：实例以回环地址进 Nacos，discover 拿到永远拨不通的地址，根因被彻底掩盖——注册一个不可达的地址比注册失败更坏。修法：`buildInstance` 对 ""/127.0.0.1/::1 报错拒注册（错误信息点名「网卡枚举失败，请显式指定 host ip」）；回退在 GetPodIP 仍是已声明行为，只在注册消费面拒绝

关联问题单：nns#66、nns#68。

## [v0.5.8] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [v0.5.7] — 2026-11-03

- fix(nns): 静态实例 Weight<=0 统一回填默认权重 10——与 registry.Apply/InfoFrom 同口径，漏配一字段不再「无可用实例」（#61）

关联问题单：nns#61。

## [v0.5.6] — 2026-11-02

- 抬嵌套模块地板 github.com/oekit/registry/nacosimpl v0.1.0 → v0.1.1（oekit/oekit#104）

## [v0.5.5] — 2026-11-01

- fix(nns): LocalServiceInit 元数据解析失败降级而非硬失败（dataId 撞车不再拖垮整个服务发现）

## [v0.5.4] — 2026-10-29

- build: 依赖地板对齐——concurrency 抬到 v0.23.2

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