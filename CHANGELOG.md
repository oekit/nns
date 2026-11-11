## [v0.8.0] — 2026-11-12

- feat(client): OnConfigChange 返回注销句柄 + 新增 OffConfigChange 注销口（#157，oekit/nns-endpoint#135 对侧）——configChangeCallbacks 改存 ConfigChangeHandle 指针包装条目（与 instanceCallbacks 同口径：Go 函数值不可比较、指针可按身份摘除），OnConfigChange 返回句柄，OffConfigChange 按句柄精确注销（nil/重复注销/条目已随 Close 清空均幂等 no-op），事件分发循环适配指针条目、「分发时现读回调列表」语义不变。长生命周期 ServiceClient 不再经回调闭包长持已废弃的 endpoint factory。**BREAKING CHANGE**：OnConfigChange 签名新增返回值 *ConfigChangeHandle——语句式调用完全兼容；把 OnConfigChange 用作函数值再调用的代码需适配（全矩阵无此用法，0.x minor 允许）。

关联问题单：nns#157。

## [v0.7.0] — 2026-11-12

- **BREAKING**（0.x minor 允许 breaking，#139）——删除三个零引用导出成员：`metadata.GRPCOption`（与 nns-endpoint 的 GRPCRequest 重叠）、`ServiceBasic.GetEndpoints`、`ServiceBasic.GetTimeoutRetryConfig`（假维度签名：protocol/serviceName 两参数从不参与匹配）。删前全工作区（含 kitex/hertz/nns-endpoint/httpx/middleware 消费仓）grep 复核零引用；`ServiceBasic.Endpoints` 字段保留，JSON 配置面不变
- **BREAKING**（契约收紧，#149）——`GetInstances`/`GetAllInstances` 返回值及 `SubscribeService` 回调入参收紧为**已发布只读快照**：调用方不得修改实例及 Metadata map（热路径零拷贝的契约代价，编译不受影响，全矩阵消费仓复核均只读使用）；持有旧快照不受后续更新影响（RCU 整体替换）
- fix(client)（#147 [P1]）：`UnsubscribeService` 与 `SubscribeService` 并发竞态——新增 per-service `unsubscribing` 状态，退订期间并发新订阅等待退订落地后再注册，修复「provider 退订拆掉窗口内新监听、本地标记已订阅后永不恢复」的服务路由静默僵死；-race 回归测试（旧实现红/新实现绿）
- feat(client)（#148，minor 增量）：新增 `ServiceClient.UnwatchService(ctx, serviceName)`——配置监听单独退订入口（摘去重标记 + StopListenConfig，失败回滚可重试，幂等 no-op），不再只能随 Close 全量终结
- refactor(client)（#141）：`GetInstances`/`GetAllInstances` 的 6 级降级链双写收敛为 `resolveInstances` 单源（discover 闭包 / cacheWrite / singleflight "all:" 前缀参数化）——优先级对齐从注释纪律变成结构保证
- refactor(client)（#142）：`Register`/`RegisterAs`/`Deregister`/`DeregisterAs` 两两复制收敛为 `registerAs`/`deregisterAs` 私有方法（no-op 守卫、#75 快照缓存口径不变）
- refactor(client)（#143）：静态实例写入口 6 处同构收敛为锁内 `setStaticLocked`（`fallbackMarkKind` 三态：运行期短路写不追改语义标记）+ 门面 `addStaticOpt`；追加/覆盖一律发布全新切片
- refactor(client)（#144）：`ServiceWithName` 兜底块三连复制与 `ServiceConfig` 主干同构收敛为 `fetchServiceConfig` + `defaultBasicWithServiceName`（吞/抛错误面与分阶段错误文案不变）
- refactor(client)（#145）：同包双 logger 统一为 `nnsLog`（component 统一 "nns"，注册路径不再漂移）
- refactor(client)（#146）：`GetRegistrar()` 标记 Deprecated 转发 `Registrar()`（双访问器收敛，kitex/hertz 适配层迁移前保留）
- perf(client)（#149 [P3]）：实例缓存改 RCU 只读快照——`updateCache` 发布即深拷贝解耦 provider 内存、整体替换；读路径零拷贝。基准（100 实例×4 元数据键）：GetInstances 缓存命中 11341 ns/op · 201 allocs/op → 30 ns/op · 0 allocs/op（BenchmarkGetInstances_CacheHit_*）
- build：Go 地板 1.24.0 → 1.26.0（ci.yml go-version 同步 '1.26'）；依赖地板对齐 logx v0.10.0、registry v0.5.15、registry/nacos v0.1.14、registry/nacosimpl v0.2.0（均矩阵地板对齐发布，无功能变更）

关联问题单：nns#139、nns#141、nns#142、nns#143、nns#144、nns#145、nns#146、nns#147、nns#148、nns#149。

## [v0.6.0] — 2026-11-11

- fix(client): RegisterAs godoc 与哨兵声明对齐 no-op success 实现（#126）——#66 起未配置 registrar 为幂等 no-op success，ErrRegistrarUnavailable 仅兼容保留、不再返回（全仓零返回点，errors.Is 分支永不命中）；顺带修正注册路径 no-op 日志误写 "deregister is a no-op" 的复制粘贴错，fallback_test 恒真断言（errors.Is(x,x) 恒真）换成「无 registrar 时恒不返回哨兵」的行为断言

关联问题单：oekit/nns#126。

- fix(client): BindServiceBasic 不再改写显式配置的 <50ms 超时（#127）——只对未配置（<=0）回填默认值，1–49ms 低延迟显式值原样保留（Warn 防呆不改写）；Retry 负值钳 0、>15 不再静默钳 15（改写必留日志）；配置中心原文与生效值一致

关联问题单：oekit/nns#127。

- fix(client): 运行期静态实例入口语义显式化（#128）——新增 AddStaticFallback/SetStaticFallback 兜底语义入口（热加备用地址不再切断注册中心发现）；AddStaticInstance/SetStaticInstances 切到静态短路时 Warn 点明后果、已有 fallbackOnly 语义标记跟随不被改写；文档补写发现路径影响

关联问题单：oekit/nns#128。

- fix(client): GetAllInstances 与 GetInstances 容错/静态语义同口径（#129）——短路判定、失败 stale/静态兜底、空结果兜底、mock/无 discoverer 降级分支全对齐（结果不写入健康视图缓存），同一配置两个发现入口不再给出不同实例面、管理/监控路径不再注册中心一错到底

关联问题单：oekit/nns#129。

## [v0.5.17] — 2026-11-10

- - build: 依赖地板对齐（第二轮发版后 floor sync，无代码变更）

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