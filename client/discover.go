package client

import (
	"context"
	"fmt"
	"github.com/oekit/registry"
	"time"
)

// GetInstances 获取服务实例，支持本地缓存和静态配置。
//
// 解析优先级（从高到低）：
//  1. 静态配置（本地开发、测试）——**仅在短路语义下**（见 WithStaticInstances
//     与 WithStaticFallback 的区别）；兜底语义下静态实例不在此步参与
//  2. Mock 模式（静态或缓存）
//  3. 新鲜缓存（由 Subscribe 推送或上一次 Discover 填充，在 cacheExpiry 内）
//  4. 注册中心 Discover（singleflight 保护，避免缓存击穿）
//  5. Discover 失败时降级使用缓存（含已过期条目）
//  6. 兜底静态实例（WithStaticFallback / FallbackRegistryFirst）——仅在
//     注册中心路径拿不到实例时参与
//
// 第 6 步让「注册中心优先、静态地址兜底」的生产形态成立：注册中心有健康
// 实例时以注册中心为准（静态地址在实例换 IP 后不会跟着变），注册中心冷启动
// 或故障时仍有静态地址兜底。
//
// 返回值契约：**空列表 + nil error** 表示「当前无可用实例，由调用方决定
// 回落」（Mock 模式下未配静态实例的名字即走这条路径）。调用方只判 err 不判
// len 会把请求路由到空——httpx 的 NNSResolver 判了 len 并回落 baseURL，
// 本包的契约以此为准。
func (c *ServiceClient) GetInstances(ctx context.Context, serviceName string) ([]registry.Instance, error) {
	// 1. 短路语义的静态配置（本地开发、测试）
	if c.staticShortCircuits(serviceName) {
		c.mu.RLock()
		static := c.staticInstances[serviceName]
		c.mu.RUnlock()
		return copyInstances(static), nil
	}

	// 2. Mock 模式
	if c.mockMode {
		return c.getInstancesFromStaticOrCache(serviceName), nil
	}

	// 3. 新鲜缓存快路径（由 Subscribe 推送或上次 Discover 填充）
	if cached := c.getInstancesFromCache(serviceName); cached != nil {
		// 缓存里存着空列表（注册中心当时返回空）时同样走兜底——
		// 否则「注册中心优先」的服务在第二次调用时会绕过兜底静态
		return c.withStaticFallback(serviceName, cached), nil
	}

	// 4. 无 Discoverer（未注入或 provider 构造失败降级）且缓存已过期或为空
	if c.discoverer == nil {
		if fb := c.staticFallbackPool(serviceName); len(fb) > 0 {
			return fb, nil
		}
		return nil, fmt.Errorf("no discoverer available and no cached instances for [%s]", serviceName)
	}

	// 5. 从注册中心获取（singleflight 保护并发）
	result, err := c.discoverSfg.Do(ctx, serviceName, func(ctx context.Context) ([]registry.Instance, error) {
		// 获取 singleflight 后再次检查缓存：可能已有其他协程完成刷新
		if cached := c.getInstancesFromCache(serviceName); cached != nil {
			return cached, nil
		}

		rawInstances, derr := c.discoverer.Discover(ctx, serviceName)
		if derr != nil {
			return nil, derr
		}

		res := rawInstances
		c.updateCache(serviceName, res)
		// 返回拷贝：缓存已持有原始切片，直接返回会让 singleflight
		// 的所有等待方共享（并可修改）缓存背后的数据
		return copyInstances(res), nil
	})
	if err != nil {
		// Discover 失败，降级使用缓存——即使已过期（stale）也比直接报错好，
		// 这正是下方 godoc 第 5 条承诺的弹性语义
		if cached := c.getStaleInstancesFromCache(serviceName); cached != nil {
			nnsLog.Warn("discover failed, falling back to stale cache", "service", serviceName, "err", err)
			return cached, nil
		}
		// 6. 兜底静态实例（注册中心不可用且无缓存可用）
		if fb := c.staticFallbackPool(serviceName); len(fb) > 0 {
			nnsLog.Warn("discover failed, falling back to static instances", "service", serviceName, "err", err)
			return fb, nil
		}
		return nil, fmt.Errorf("discover instances [%s]: %w", serviceName, err)
	}

	// 6. 注册中心返回空列表 → 兜底静态实例
	return c.withStaticFallback(serviceName, result), nil
}

// GetAllInstances 获取指定服务的全部实例列表（包括不健康的实例）。
// 使用 singleflight 保护并发请求，避免缓存击穿。
func (c *ServiceClient) GetAllInstances(ctx context.Context, serviceName string) ([]registry.Instance, error) {
	if c.mockMode || c.discoverer == nil {
		return c.getInstancesFromStaticOrCache(serviceName), nil
	}

	result, err := c.discoverSfg.Do(ctx, "all:"+serviceName, func(ctx context.Context) ([]registry.Instance, error) {
		rawInstances, derr := c.discoverer.DiscoverAll(ctx, serviceName)
		if derr != nil {
			return nil, fmt.Errorf("discover all instances [%s]: %w", serviceName, derr)
		}
		return rawInstances, nil
	})
	if err != nil {
		return nil, err
	}
	// 返回拷贝，避免 singleflight 等待方共享同一可变切片
	return copyInstances(result), nil
}

// getInstancesFromStaticOrCache 从静态配置或缓存获取实例
func (c *ServiceClient) getInstancesFromStaticOrCache(serviceName string) []registry.Instance {
	c.mu.RLock()
	instances, ok := c.staticInstances[serviceName]
	c.mu.RUnlock()
	if ok && len(instances) > 0 {
		return copyInstances(instances)
	}
	return c.getInstancesFromCache(serviceName)
}

// getInstancesFromCache 从本地缓存获取实例（过期条目视为未命中）
func (c *ServiceClient) getInstancesFromCache(serviceName string) []registry.Instance {
	return c.cacheLookup(serviceName, false)
}

// getStaleInstancesFromCache 返回缓存实例并忽略过期时间。
// 仅用于注册中心故障时的降级：宁可服务可能过期的实例列表，也不硬报错。
func (c *ServiceClient) getStaleInstancesFromCache(serviceName string) []registry.Instance {
	return c.cacheLookup(serviceName, true)
}

// cacheLookup 读取实例缓存；ignoreExpiry=true 时跳过过期检查。
func (c *ServiceClient) cacheLookup(serviceName string, ignoreExpiry bool) []registry.Instance {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()

	instances, ok := c.instanceCache[serviceName]
	if !ok {
		return nil
	}

	// 检查缓存是否过期
	if !ignoreExpiry {
		lastUpdate, ok := c.cacheLastUpdate[serviceName]
		if ok && time.Since(lastUpdate) > c.cacheExpiry {
			nnsLog.Debug("cache expired", "service", serviceName)
			return nil
		}
	}

	return copyInstances(instances)
}

// copyInstances 深拷贝实例列表（含 Metadata map），
// 避免调用方修改污染内部缓存或静态配置。
func copyInstances(instances []registry.Instance) []registry.Instance {
	result := make([]registry.Instance, len(instances))
	copy(result, instances)
	for i := range result {
		if result[i].Metadata != nil {
			meta := make(map[string]string, len(result[i].Metadata))
			for k, v := range result[i].Metadata {
				meta[k] = v
			}
			result[i].Metadata = meta
		}
	}
	return result
}

// updateCache 更新本地缓存
func (c *ServiceClient) updateCache(serviceName string, instances []registry.Instance) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	c.instanceCache[serviceName] = instances
	c.cacheLastUpdate[serviceName] = time.Now()
	nnsLog.Debug("cache updated", "service", serviceName, "count", len(instances))
}
