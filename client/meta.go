package client

import (
	"context"
	"encoding/json"
	"github.com/oekit/nns/metadata"
	"github.com/oekit/registry"
)

// BindServiceBasic 解析服务全名并设置服务基本配置的默认值。
func (c *ServiceClient) BindServiceBasic(serviceName string, basic *metadata.ServiceBasic) {
	serviceInfo := metadata.ParseServiceName(serviceName)
	if serviceInfo != nil {
		basic.ServiceName = *serviceInfo
	}

	if basic.ConnTimeout < 50 {
		basic.ConnTimeout = 1000
	}
	if basic.ReadTimeout < 50 {
		basic.ReadTimeout = 5000
	}
	if basic.WriteTimeout < 50 {
		basic.WriteTimeout = 5000
	}
	if basic.Retry > 15 {
		basic.Retry = 15
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

// AddStaticInstance 添加静态服务实例
func (c *ServiceClient) AddStaticInstance(serviceName string, instances ...registry.Instance) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.staticInstances[serviceName] = append(c.staticInstances[serviceName], instances...)
	nnsLog.Info("added static instances", "service", serviceName, "count", len(instances))
}

// SetStaticInstances 设置静态服务实例（覆盖）
func (c *ServiceClient) SetStaticInstances(serviceName string, instances []registry.Instance) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.staticInstances[serviceName] = instances
	nnsLog.Info("set static instances", "service", serviceName, "count", len(instances))
}
