package client

import (
	"context"
	"fmt"
	"maps"
	"slices"
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
//
// # 快照契约（#149，原「每请求深拷贝」契约收紧）
//
// 返回的是内部**已发布只读快照**的引用：本包对实例数据只做「发布时一次
// 深拷贝 + 整体替换」（RCU），绝不原地改写任何已发布的快照，也绝不外泄
// provider/调用方拥有的可变内存。因此：
//   - 调用方**不得修改**返回的实例（含 Metadata map）——改动对本进程后续
//     读者可见（热路径零拷贝的代价）；需要私有可能改的副本请自行克隆；
//   - 持有旧快照的调用方不受后续推送/发现更新影响（更新整体替换，旧快照
//     内容恒定）；
//   - 缓存命中热路径零分配（此前每请求深拷贝实例列表 + 每实例一个
//     Metadata map，nns-endpoint 每请求调用的场景是可测量的稳态分配）。
func (c *ServiceClient) GetInstances(ctx context.Context, serviceName string) ([]registry.Instance, error) {
	return c.resolveInstances(ctx, serviceName, "discover",
		func(ctx context.Context, name string) ([]registry.Instance, error) {
			return c.discoverer.Discover(ctx, name)
		}, true)
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
// 由调用方决定回落」；返回的也是**已发布只读快照**（见 GetInstances 的
// 快照契约，#149）——DiscoverAll 的结果在 singleflight 出口做一次拷贝与
// provider 内存解耦，并发等待方共享同一只读快照。
func (c *ServiceClient) GetAllInstances(ctx context.Context, serviceName string) ([]registry.Instance, error) {
	return c.resolveInstances(ctx, serviceName, "discover all",
		func(ctx context.Context, name string) ([]registry.Instance, error) {
			return c.discoverer.DiscoverAll(ctx, name)
		}, false)
}

// resolveInstances 是 GetInstances / GetAllInstances 的公共实现（#141）：
// 此前两条 6 级降级链整段双写，靠注释纪律对齐（#129）；现在优先级、降级
// 与兜底收敛为单源，差异全部参数化：
//
//   - discover：Discover vs DiscoverAll 闭包；
//   - cacheWrite：健康视图写实例缓存（并在 singleflight 内复查缓存），
//     全量视图不写缓存也不读缓存快路径（仅在无 Discoverer 的降级态读）；
//   - singleflight key：cacheWrite=false 时加 "all:" 前缀，与健康视图的
//     flight 分开；
//   - op：日志与错误包装里的动作名（"discover" / "discover all"）。
func (c *ServiceClient) resolveInstances(
	ctx context.Context,
	serviceName string,
	op string,
	discover func(context.Context, string) ([]registry.Instance, error),
	cacheWrite bool,
) ([]registry.Instance, error) {
	// 1. 短路语义的静态配置（本地开发、测试）
	if c.staticShortCircuits(serviceName) {
		c.mu.RLock()
		static := c.staticInstances[serviceName]
		c.mu.RUnlock()
		return static, nil
	}

	// 2. Mock 模式
	if c.mockMode {
		return c.getInstancesFromStaticOrCache(serviceName), nil
	}

	// 3. 新鲜缓存快路径（由 Subscribe 推送或上次 Discover 填充）：
	//    健康视图（cacheWrite）恒可读——缓存里存着空列表（注册中心当时返回
	//    空）时同样走兜底，否则「注册中心优先」的服务在第二次调用时会绕过
	//    兜底静态；全量视图只在无 Discoverer 的降级态读缓存（有 Discoverer
	//    时每次直通注册中心，缓存只存健康视图、不代全量）。
	if cacheWrite || c.discoverer == nil {
		if cached := c.getInstancesFromCache(serviceName); cached != nil {
			return c.withStaticFallback(serviceName, cached), nil
		}
	}

	// 4. 无 Discoverer（未注入或 provider 构造失败降级）且缓存已过期或为空
	if c.discoverer == nil {
		if fb := c.staticFallbackPool(serviceName); len(fb) > 0 {
			return fb, nil
		}
		return nil, fmt.Errorf("no discoverer available and no cached instances for [%s]", serviceName)
	}

	// 5. 从注册中心获取（singleflight 保护并发）
	key := serviceName
	if !cacheWrite {
		key = "all:" + serviceName
	}
	result, err := c.discoverSfg.Do(ctx, key, func(ctx context.Context) ([]registry.Instance, error) {
		// 获取 singleflight 后再次检查缓存：可能已有其他协程完成刷新
		// （仅健康视图——全量视图不写缓存，无此复查语义）
		if cacheWrite {
			if cached := c.getInstancesFromCache(serviceName); cached != nil {
				return cached, nil
			}
		}

		rawInstances, derr := discover(ctx, serviceName)
		if derr != nil {
			return nil, fmt.Errorf("%s instances [%s]: %w", op, serviceName, derr)
		}

		if cacheWrite {
			// 发布（内部一次深拷贝，与 provider 内存解耦）并返回同一只读快照
			return c.updateCache(serviceName, rawInstances), nil
		}
		// 全量视图：出口一次拷贝与 provider 内存解耦，singleflight 等待方
		// 共享同一只读快照
		return copyInstances(rawInstances), nil
	})
	if err != nil {
		// 注册中心路径失败，降级使用缓存——即使已过期（stale）也比直接报错好，
		// 这正是 godoc 第 5 条承诺的弹性语义
		if cached := c.getStaleInstancesFromCache(serviceName); cached != nil {
			nnsLog.Warn(op+" failed, falling back to stale cache", "service", serviceName, logx.ErrKey, err)
			return cached, nil
		}
		// 6. 兜底静态实例（注册中心不可用且无缓存可用）
		if fb := c.staticFallbackPool(serviceName); len(fb) > 0 {
			nnsLog.Warn(op+" failed, falling back to static instances", "service", serviceName, logx.ErrKey, err)
			return fb, nil
		}
		return nil, err
	}

	// 6. 注册中心返回空列表 → 兜底静态实例
	return c.withStaticFallback(serviceName, result), nil
}

// getInstancesFromStaticOrCache 从静态配置或缓存获取实例（只读快照引用，
// #149）。
func (c *ServiceClient) getInstancesFromStaticOrCache(serviceName string) []registry.Instance {
	c.mu.RLock()
	instances, ok := c.staticInstances[serviceName]
	c.mu.RUnlock()
	if ok && len(instances) > 0 {
		return instances
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
//
// 返回**已发布只读快照**的引用（#149）：缓存条目由 updateCache 在发布时
// 一次深拷贝物化、此后整体替换、无人原地改写，读路径零拷贝——此前每读必
// copyInstances（每请求分配新切片 + 每实例一个 Metadata map），是
// nns-endpoint 每请求调 GetInstances 场景下的稳态分配源。
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

	return instances
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

// copyInstances 深拷贝实例列表（含 Metadata map）。
//
// 仅在**发布边界**使用（#149）：缓存写入（与 provider 内存解耦）、静态表
// 写入（与调用方入参解耦）、DiscoverAll 出口。读路径直接引用已发布快照，
// 不再每读拷贝。
func copyInstances(instances []registry.Instance) []registry.Instance {
	result := slices.Clone(instances)
	for i := range result {
		result[i].Metadata = maps.Clone(result[i].Metadata)
	}
	return result
}

// updateCache 发布新的实例缓存快照并返回它。
//
// 发布时一次深拷贝（与 provider 推送 / Discover 结果的私有内存解耦——
// 上游复用缓冲不再可能腐蚀缓存），整体替换旧条目（RCU，#149）：已发出的
// 旧快照引用不受影响，读路径拿到的是不再被本包改写的只读数据。
func (c *ServiceClient) updateCache(serviceName string, instances []registry.Instance) []registry.Instance {
	snap := copyInstances(instances)

	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	c.instanceCache[serviceName] = snap
	c.cacheLastUpdate[serviceName] = time.Now()
	nnsLog.Debug("cache updated", "service", serviceName, "count", len(snap))
	return snap
}
