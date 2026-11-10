package client

import (
	"context"
	"fmt"
	"time"

	"github.com/oekit/logx"
	"github.com/oekit/registry"
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
//
// 与 GetInstances 同一套容错/静态语义（#129——此前两个入口整体分叉：短路
// 服务照样打注册中心、失败无兜底、空结果不兜底、降级分支把兜底服务当短路
// 返回，同一配置两个入口给出不同实例面），差别只有两点：
//
//   - 取「全部实例」走 DiscoverAll（含不健康实例）；
//   - 结果**不写入**实例缓存（实例缓存只存健康视图，供 GetInstances 路由）。
//
// 优先序（与 GetInstances 逐级对齐）：
//  1. 短路语义的静态配置（同 GetInstances 第 1 级）
//  2. Mock 模式（同 GetInstances 第 2 级）
//  3. 无 Discoverer：新鲜缓存 → 兜底静态 → 报错（同 GetInstances 第 3/4 级）
//  4. DiscoverAll（singleflight 保护，"all:" 前缀与 GetInstances 的
//     flight 分开；失败降级 stale 缓存 → 兜底静态 → 报错，同第 5/6 级）
//  5. 空结果兜底静态（同 GetInstances 第 6 级）
//
// 返回值契约与 GetInstances 一致：空列表 + nil error 表示「当前无可用实例，
// 由调用方决定回落」。
func (c *ServiceClient) GetAllInstances(ctx context.Context, serviceName string) ([]registry.Instance, error) {
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

	// 3. 无 Discoverer（未注入或 provider 构造失败降级）：与 GetInstances
	// 同口径——新鲜缓存优先，兜底静态实例不被当短路返回
	if c.discoverer == nil {
		if cached := c.getInstancesFromCache(serviceName); cached != nil {
			return c.withStaticFallback(serviceName, cached), nil
		}
		if fb := c.staticFallbackPool(serviceName); len(fb) > 0 {
			return fb, nil
		}
		return nil, fmt.Errorf("no discoverer available and no cached instances for [%s]", serviceName)
	}

	// 4. 从注册中心获取全部实例（singleflight 保护并发，避免缓存击穿）
	result, err := c.discoverSfg.Do(ctx, "all:"+serviceName, func(ctx context.Context) ([]registry.Instance, error) {
		rawInstances, derr := c.discoverer.DiscoverAll(ctx, serviceName)
		if derr != nil {
			return nil, fmt.Errorf("discover all instances [%s]: %w", serviceName, derr)
		}
		return rawInstances, nil
	})
	if err != nil {
		// DiscoverAll 失败，降级使用缓存——即使已过期（stale）也比直接报错好，
		// 与 GetInstances 第 5/6 级同口径
		if cached := c.getStaleInstancesFromCache(serviceName); cached != nil {
			nnsLog.Warn("discover all failed, falling back to stale cache", "service", serviceName, logx.ErrKey, err)
			return cached, nil
		}
		if fb := c.staticFallbackPool(serviceName); len(fb) > 0 {
			nnsLog.Warn("discover all failed, falling back to static instances", "service", serviceName, logx.ErrKey, err)
			return fb, nil
		}
		return nil, err
	}

	// 5. 注册中心返回空列表 → 兜底静态实例（同 GetInstances 第 6 级）。
	// 返回拷贝，避免 singleflight 等待方共享同一可变切片
	return c.withStaticFallback(serviceName, copyInstances(result)), nil
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

// defaultStaticWeight 是静态实例的默认权重，与注册侧口径一致：
// registry.Apply 的 RegisterOption.Weight 默认 10，registry.InfoFrom 对
// weight<=0 回退 defaultWeight（注册侧传入同为 10）。
const defaultStaticWeight = 10

// normalizeStaticInstances 归一化静态实例列表：拷贝入参（避免调用方后续
// 修改污染内部静态表），并对 Weight<=0 的实例回填 defaultStaticWeight。
//
// 与注册侧对称（registry.Apply / registry.InfoFrom 的 defaultWeight=10）：
// 静态配置漏写权重字段不等于「摘流」——kitex 加权均衡（weighted_balancer）
// 对 weight<=0 的实例整只跳过，不回填会让「漏配一个字段」静默变成
// 「无可用实例」。静态实例入口不提供「零权重 = drain」语义，需要摘流
// 请直接移除实例。
func normalizeStaticInstances(instances []registry.Instance) []registry.Instance {
	result := copyInstances(instances)
	for i := range result {
		if result[i].Weight <= 0 {
			result[i].Weight = defaultStaticWeight
		}
	}
	return result
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
