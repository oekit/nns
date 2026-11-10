package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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

// ServiceWithName 根据服务全名获取服务基本配置。
func (c *ServiceClient) ServiceWithName(ctx context.Context, serviceName string) metadata.ServiceBasic {
	// Mock 模式或有本地配置
	if c.mockMode {
		if basic, ok := c.localServiceConfig[serviceName]; ok {
			return *basic
		}
		return *metadata.NewServiceBasic()
	}

	basic := metadata.NewServiceBasic()

	if c.configStore == nil {
		serviceInfo := metadata.ParseServiceName(serviceName)
		if serviceInfo != nil {
			b := metadata.NewServiceBasic()
			b.ServiceName = *serviceInfo
			return *b
		}
		return *metadata.NewServiceBasic()
	}

	content, err := c.configStore.GetConfig(ctx, serviceName, c.configGroup)
	if err != nil {
		nnsLog.Warn("get service config failed", "service", serviceName, "err", err)
		serviceInfo := metadata.ParseServiceName(serviceName)
		if serviceInfo != nil {
			b := metadata.NewServiceBasic()
			b.ServiceName = *serviceInfo
			return *b
		}
		return *metadata.NewServiceBasic()
	}

	if content != "" {
		if err := json.Unmarshal([]byte(content), basic); err != nil {
			nnsLog.Warn("unmarshal service config failed", "service", serviceName, "err", err)
		}
	}

	c.BindServiceBasic(serviceName, basic)
	return *basic
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
		if basic, ok := c.localServiceConfig[serviceName]; ok {
			return *basic, nil
		}
		return *metadata.NewServiceBasic(), nil
	}
	if c.configStore == nil {
		return metadata.ServiceBasic{}, ErrConfigStoreUnavailable
	}

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

// GetLocalService 获取本地服务的基本配置信息。
func (c *ServiceClient) GetLocalService() *metadata.ServiceBasic {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.localService
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
func (c *ServiceClient) GetRegistrar() registry.Registrar {
	return c.registrar
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

// AddStaticInstance 添加静态服务实例。
// 权重与注册侧同口径：Weight<=0 回填默认权重 10（见 normalizeStaticInstances）。
func (c *ServiceClient) AddStaticInstance(serviceName string, instances ...registry.Instance) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.staticInstances[serviceName] = append(c.staticInstances[serviceName], normalizeStaticInstances(instances)...)
	nnsLog.Info("added static instances", "service", serviceName, "count", len(instances))
}

// SetStaticInstances 设置静态服务实例（覆盖）。
// 权重与注册侧同口径：Weight<=0 回填默认权重 10（见 normalizeStaticInstances）。
func (c *ServiceClient) SetStaticInstances(serviceName string, instances []registry.Instance) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.staticInstances[serviceName] = normalizeStaticInstances(instances)
	nnsLog.Info("set static instances", "service", serviceName, "count", len(instances))
}
