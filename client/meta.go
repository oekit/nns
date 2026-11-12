package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/oekit/logx"
	"github.com/oekit/nns/metadata"
	"github.com/oekit/registry"
)

// ErrConfigStoreUnavailable 在需要真实配置拉取的入口（ServiceConfig）被调、
// 但未注入 ConfigStore（mock 模式之外）时返回（#77）。
var ErrConfigStoreUnavailable = errors.New("nns: config store not available")

// BindServiceBasic 解析服务全名并设置服务基本配置的默认值。
//
// 只对**未配置**（<=0）的超时回填默认值（ConnTimeout 1000ms / ReadTimeout
// 5000ms / WriteTimeout 5000ms）；显式配置的取值原样保留——1–49ms 的低延迟
// 超时不再被改写成默认值（#127：此前 <50 一律静默钳成 1000/5000ms，20ms
// 的配置被吞 50 倍且零日志），仅打 Warn 防呆、不改写。
//
// Retry：负值钳 0（负重试无意义；这是仅存的改写，必留日志）；>15 原样保留
// 并 Warn——不再静默钳 15（#127）。
//
// 本函数是配置消费的必经点（LocalServiceInit / LoadDepService /
// ServiceConfig / ServiceWithName / WatchService 热更回调），口径统一在
// 这里收口：配置中心原文与生效值一致。
func (c *ServiceClient) BindServiceBasic(serviceName string, basic *metadata.ServiceBasic) {
	serviceInfo := metadata.ParseServiceName(serviceName)
	if serviceInfo != nil {
		basic.ServiceName = *serviceInfo
	}

	bindTimeout := func(field string, v *int, def int) {
		switch {
		case *v <= 0:
			nnsLog.Debug("timeout not configured, backfilled to default",
				"service", serviceName, "field", field, "default", def)
			*v = def
		case *v < 50:
			nnsLog.Warn("suspiciously low timeout kept as-is (not rewritten)",
				"service", serviceName, "field", field, "value", *v)
		}
	}
	bindTimeout("conn_timeout", &basic.ConnTimeout, 1000)
	bindTimeout("read_timeout", &basic.ReadTimeout, 5000)
	bindTimeout("write_timeout", &basic.WriteTimeout, 5000)

	switch {
	case basic.Retry < 0:
		nnsLog.Warn("negative retry clamped to 0",
			"service", serviceName, "value", basic.Retry)
		basic.Retry = 0
	case basic.Retry > 15:
		nnsLog.Warn("retry above recommended max kept as-is (not clamped)",
			"service", serviceName, "value", basic.Retry)
	}
}

// defaultBasicWithServiceName 构造带服务名维度的默认配置兜底值（#144）：
// 服务全名可解析（5 段规范形）时填充 ServiceName 维度，否则返回纯默认值。
// ServiceWithName 的两个静默降级分支（无 ConfigStore / 拉取失败）共用。
func defaultBasicWithServiceName(serviceName string) metadata.ServiceBasic {
	if serviceInfo := metadata.ParseServiceName(serviceName); serviceInfo != nil {
		b := metadata.NewServiceBasic()
		b.ServiceName = *serviceInfo
		return *b
	}
	return *metadata.NewServiceBasic()
}

// localServiceConfigOrDefault 取构造期注入的本地服务配置（Mock 模式用）；
// 未配置返回默认值。
func (c *ServiceClient) localServiceConfigOrDefault(serviceName string) metadata.ServiceBasic {
	if basic, ok := c.localServiceConfig[serviceName]; ok {
		return *basic
	}
	return *metadata.NewServiceBasic()
}

// fetchServiceConfig 是 ServiceConfig / ServiceWithName 的共同主干（#144）：
// GetConfig → JSON 解析 → BindServiceBasic 收口。错误按阶段包装后返回，
// 由调用方按各自错误面处理（ServiceConfig 原样上抛 / ServiceWithName 静默
// 降级）。仅限 configStore 非 nil 时调用。
func (c *ServiceClient) fetchServiceConfig(ctx context.Context, serviceName string) (metadata.ServiceBasic, error) {
	content, err := c.configStore.GetConfig(ctx, serviceName, c.configGroup)
	if err != nil {
		return metadata.ServiceBasic{}, fmt.Errorf("get service config [%s]: %w", serviceName, err)
	}
	basic := metadata.NewServiceBasic()
	if content != "" {
		if err := json.Unmarshal([]byte(content), basic); err != nil {
			return metadata.ServiceBasic{}, fmt.Errorf("unmarshal service config [%s]: %w", serviceName, err)
		}
	}
	c.BindServiceBasic(serviceName, basic)
	return *basic, nil
}

// ServiceWithName 根据服务全名获取服务基本配置。
//
// 软依赖语义：配置拉取失败/JSON 解析失败时静默降级——日志 Warn 后返回
// 默认超时值（硬失败面见 ServiceConfig）。主干与 ServiceConfig 共享
// fetchServiceConfig（#144）。
func (c *ServiceClient) ServiceWithName(ctx context.Context, serviceName string) metadata.ServiceBasic {
	// Mock 模式或有本地配置
	if c.mockMode {
		return c.localServiceConfigOrDefault(serviceName)
	}

	if c.configStore == nil {
		return defaultBasicWithServiceName(serviceName)
	}

	basic, err := c.fetchServiceConfig(ctx, serviceName)
	if err != nil {
		nnsLog.Warn("get service config failed, using default", "service", serviceName, logx.ErrKey, err)
		return defaultBasicWithServiceName(serviceName)
	}
	return basic
}

// ServiceConfig 拉取指定服务的配置并返回**真实错误**（#77）。
//
// 与 ServiceWithName 的差别只在错误面：
//   - ServiceWithName 把配置拉取失败/JSON 解析失败静默降级为默认超时值
//     （软依赖消费方要「有默认值就能跑」）；
//   - 本方法把错误原样上抛——需要区分「拉到了配置」与「拿到的是兜底默认值」
//     的调用方（如强依赖配置的启动校验）用它取到真实结果。
//
// 错误语义：
//   - GetConfig 失败（配置中心不可达等）：返回包装后的原始错误；
//   - 配置内容 JSON 损坏：返回解析错误（配置写坏应显式暴露，而非静默
//     降级默认值）；
//   - 未注入 ConfigStore（mock 之外）：返回 ErrConfigStoreUnavailable——
//     此前该场景在 GetService 门面上表现为「恒成功 + 默认超时值」；
//   - 配置未发布（content 为空）不算错误：返回默认值 + nil，与
//     ServiceWithName 一致；
//   - Mock 模式：取 localServiceConfig，未配置则默认值，不报错。
func (c *ServiceClient) ServiceConfig(ctx context.Context, serviceName string) (metadata.ServiceBasic, error) {
	if c.mockMode {
		return c.localServiceConfigOrDefault(serviceName), nil
	}
	if c.configStore == nil {
		return metadata.ServiceBasic{}, ErrConfigStoreUnavailable
	}

	basic, err := c.fetchServiceConfig(ctx, serviceName)
	if err != nil {
		return metadata.ServiceBasic{}, err
	}
	return basic, nil
}

// GetLocalService 获取本地服务的基本配置信息。
//
// 返回内部状态的**浅拷贝快照**（#154）：此前直接导出内部 *ServiceBasic
// 活指针，WatchService 热更回调会整体替换/改写该对象（config.go），调用方
// 经返回指针写字段（如改 ConnTimeout）会与并发 GetServiceBasic 的解引用
// 构成无锁数据竞争（别名写穿面）。快照口径与 GetServiceBasic / GetDepService
// 统一：结构体浅拷贝——嵌套的指针/map 字段（如 TimeoutRetryCfg）与内部
// 状态共享，调用方不得修改。未 Init 时返回 nil（既有行为不变）。
func (c *ServiceClient) GetLocalService() *metadata.ServiceBasic {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.localService == nil {
		return nil
	}
	basic := *c.localService
	return &basic
}

// GetServiceBasic 返回指定服务的内存配置（不发起网络请求）。
//
// 优先从已加载的 DepService / LocalService 中读取（由 Init 中的 WatchService 保证热更新），
// 未加载时返回带默认值的 ServiceBasic。供 endpoint 在每次调用时快速读取超时等配置。
func (c *ServiceClient) GetServiceBasic(serviceName string) metadata.ServiceBasic {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if dep, ok := c.depService[serviceName]; ok && dep != nil {
		return *dep
	}
	if c.localService != nil && c.localService.ServiceName.FullName == serviceName {
		return *c.localService
	}
	return *metadata.NewServiceBasic()
}

// GetLocalColor 获取本地服务的灰度标识颜色。
// 颜色来自本地服务元数据，可通过 SetLocalServiceMeta 设置。
func (c *ServiceClient) GetLocalColor() string {
	meta := c.localServiceMeta.Load()
	if meta == nil {
		return ""
	}
	if color, ok := (*meta)["color"]; ok {
		return color
	}
	return ""
}

// SetLocalServiceMeta 设置本地服务元数据（如 color/region 灰度标识）。
// 并发安全；传入 nil 等效于清空。典型来源是启动时的环境变量或平台注入。
func (c *ServiceClient) SetLocalServiceMeta(meta map[string]string) {
	copied := make(map[string]string, len(meta))
	for k, v := range meta {
		copied[k] = v
	}
	c.localServiceMeta.Store(&copied)
}

// GetLocalRegion 获取本地服务所在的区域信息。
func (c *ServiceClient) GetLocalRegion() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.localService != nil {
		return c.localService.ServiceName.Region
	}
	return ""
}

// GetRegistrar 返回内部的 registrar 接口（供 nns/kitex 适配器使用）。
//
// Deprecated: 与 Registrar() 完全等价（#146 双访问器收敛）。新代码请用
// Registrar()；kitex/hertz 适配层迁移完成前本转发保留，之后随 major 删除。
func (c *ServiceClient) GetRegistrar() registry.Registrar {
	return c.Registrar()
}

// GetDepService 返回依赖服务配置的快照（并发安全）。
// 注意是结构体浅拷贝：嵌套的指针/map 字段（如 TimeoutRetryCfg）与内部状态共享，
// 调用方不得修改；并发安全由 WatchService 对字段的整体替换保证。
// 建议使用此方法而非直接访问内部 depService 字段。
func (c *ServiceClient) GetDepService() map[string]*metadata.ServiceBasic {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.depService) == 0 {
		return nil
	}
	out := make(map[string]*metadata.ServiceBasic, len(c.depService))
	for k, v := range c.depService {
		if v != nil {
			copy := *v
			out[k] = &copy
		}
	}
	return out
}

// fallbackMarkKind 描述写入口对 fallbackOnly 语义标记的动作（#143 收敛前
// 由各入口各自维护，行为差异保留）。
type fallbackMarkKind int

const (
	// fallbackKeep 不改语义标记：运行期短路写入口（Add/SetStaticInstance）
	// ——已有兜底语义的服务被写入后仍是兜底（#128「语义标记跟随」）。
	fallbackKeep fallbackMarkKind = iota
	// fallbackMark 标记兜底语义（Add/SetStaticFallback、WithStaticFallback）。
	fallbackMark
	// fallbackClear 清除标记恢复短路语义（构造期 WithStaticInstances：
	// 同一服务与 WithStaticFallback 重复配置时以最后一次调用为准）。
	fallbackClear
)

// setStaticLocked 是全部静态实例写入口（With/Add/Set × 短路/兜底 × 追加/覆盖，
// 共 6 处，#143）的锁内公共实现：归一化（normalizeStaticInstances，拷贝入参
// 并回填默认权重）、写表、维护 fallbackOnly 语义标记。
//
// 调用方必须已持有 c.mu（写锁）。appendMode=true 在原条目后追加，false 覆盖。
// 追加与覆盖都**发布全新切片**（不原地改写既有 backing array）——读路径
// （#149）把静态表当作已发布只读快照直接引用，任何原地写入都会污染已
// 发出的快照。
func (c *ServiceClient) setStaticLocked(serviceName string, instances []registry.Instance, appendMode bool, mark fallbackMarkKind) {
	if c.staticInstances == nil {
		c.staticInstances = make(map[string][]registry.Instance)
	}
	normalized := normalizeStaticInstances(instances)
	var next []registry.Instance
	if appendMode {
		prev := c.staticInstances[serviceName]
		next = make([]registry.Instance, 0, len(prev)+len(normalized))
		next = append(next, prev...)
		next = append(next, normalized...)
	} else {
		next = normalized
	}
	c.staticInstances[serviceName] = next

	switch mark {
	case fallbackMark:
		if c.fallbackOnly == nil {
			c.fallbackOnly = make(map[string]bool)
		}
		c.fallbackOnly[serviceName] = true
	case fallbackClear:
		if c.fallbackOnly != nil {
			delete(c.fallbackOnly, serviceName)
		}
	}
}

// warnStaticShortCircuitFlipLocked 在运行期写入口把服务从「查注册中心」切到
// 「静态短路」时 Warn 点明后果（#128）：仅当此前不短路、现在短路时告警。
// 调用方必须已持有 c.mu。
func (c *ServiceClient) warnStaticShortCircuitFlipLocked(serviceName string, wasShort bool) {
	if wasShort || !c.staticShortCircuitsLocked(serviceName) {
		return
	}
	nnsLog.Warn("static instances now short-circuit registry discovery for this service; "+
		"registry up/down, weight and health changes no longer apply — "+
		"use AddStaticFallback/SetStaticFallback for backup addresses, or SetStaticInstances(nil) to restore",
		"service", serviceName)
}

// AddStaticInstance 添加静态服务实例（**短路**语义，与 WithStaticInstances
// 一致）。
//
// ⚠️ 会改变发现路径（#128）：FallbackStaticFirst（缺省）下，此前没有静态
// 配置的服务被写入后即**短路注册中心**——实例上下线、权重调整、健康摘除
// 不再反映到 GetInstances/GetAllInstances。运行期热加「备用地址」请改用
// AddStaticFallback（兜底语义，注册中心发现保持不变）；确要切到短路时本
// 函数会打 Warn 点明后果。已有语义标记跟随（fallbackOnly 不被改写）：
// 兜底语义的服务追加实例仍是兜底。
//
// 权重与注册侧同口径：Weight<=0 回填默认权重 10（见 normalizeStaticInstances）。
func (c *ServiceClient) AddStaticInstance(serviceName string, instances ...registry.Instance) {
	c.mu.Lock()
	defer c.mu.Unlock()

	wasShort := c.staticShortCircuitsLocked(serviceName)
	c.setStaticLocked(serviceName, instances, true, fallbackKeep)
	nnsLog.Info("added static instances", "service", serviceName, "count", len(instances))
	c.warnStaticShortCircuitFlipLocked(serviceName, wasShort)
}

// SetStaticInstances 设置静态服务实例（覆盖，**短路**语义，与
// WithStaticInstances 一致）。
//
// ⚠️ 同 AddStaticInstance：会把该服务切到静态短路（切路径时 Warn）；
// 热加备用地址请用 SetStaticFallback。已有语义标记跟随（fallbackOnly
// 不被改写）。
//
// 权重与注册侧同口径：Weight<=0 回填默认权重 10（见 normalizeStaticInstances）。
func (c *ServiceClient) SetStaticInstances(serviceName string, instances []registry.Instance) {
	c.mu.Lock()
	defer c.mu.Unlock()

	wasShort := c.staticShortCircuitsLocked(serviceName)
	c.setStaticLocked(serviceName, instances, false, fallbackKeep)
	nnsLog.Info("set static instances", "service", serviceName, "count", len(instances))
	c.warnStaticShortCircuitFlipLocked(serviceName, wasShort)
}

// AddStaticFallback 追加**兜底**静态实例（与 WithStaticFallback 一致的语义）。
//
// 与 AddStaticInstance 的区别只在参与时机：本入口把该服务标记为兜底语义，
// **不改变发现路径**——注册中心仍是第一数据源，只有注册中心路径失败或返回
// 空时静态实例才参与取值。运行期热加备用地址（管理面/脚本）用它，不会切断
// 注册中心发现（#128）。
//
// 权重与注册侧同口径：Weight<=0 回填默认权重 10（见 normalizeStaticInstances）。
func (c *ServiceClient) AddStaticFallback(serviceName string, instances ...registry.Instance) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.setStaticLocked(serviceName, instances, true, fallbackMark)
	nnsLog.Info("added static fallback instances (registry discovery preserved)",
		"service", serviceName, "count", len(instances))
}

// SetStaticFallback 设置**兜底**静态实例（覆盖，与 WithStaticFallback 一致
// 的语义）。
//
// 与 SetStaticInstances 的区别只在参与时机：本入口把该服务标记为兜底语义，
// **不改变发现路径**（#128）。权重口径与 AddStaticFallback 一致。
func (c *ServiceClient) SetStaticFallback(serviceName string, instances []registry.Instance) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.setStaticLocked(serviceName, instances, false, fallbackMark)
	nnsLog.Info("set static fallback instances (registry discovery preserved)",
		"service", serviceName, "count", len(instances))
}
