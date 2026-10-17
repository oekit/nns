# Changelog — oekit/nns

由 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来。所有 notable 变更记录如下。


## [v0.3.4] — 2026-10-17

- fix(deps): 内部依赖地板全量对齐最新——concurrency v0.5.2、core v0.6.0、logx v0.5.0、registry v0.3.1；go 最低版本降至 1.24.0——prometheus 家族（client_golang v1.23.2/procfs v0.19.2）与 x/crypto v0.48/x/net v0.50/x/sys v0.41/x/text v0.34 锁 go1.24 兼容版

## [v0.1.0] — 2026-10-15

自 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来的初始公开版本，完成拆分稳定化：模块路径统一为 `github.com/oekit/nns`，多轮拆分/合并收敛至当前模块边界，组织基建（共享 CI workflow、golangci v2、Dependabot）就位。