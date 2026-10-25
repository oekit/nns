package client

import (
	"github.com/oekit/registry"
)

// FallbackOrder 决定静态实例与注册中心的取值优先序。
//
// 静态实例有两种角色，差别只在「什么时候参与取值」：
//
//   - 短路静态（WithStaticInstances，FallbackStaticFirst 下的默认）：为服务
//     配了静态实例就直接返回，不再查询注册中心。适合本地开发、测试。
//   - 兜底静态（WithStaticFallback，或全局 FallbackRegistryFirst 下的
//     静态实例）：先查注册中心，注册中心路径失败或返回空时才用静态实例
//     兜底。适合生产形态——静态地址（如 k8s Service DNS）只是最后一层，
//     注册中心有健康实例时以注册中心为准。
type FallbackOrder int

const (
	// FallbackStaticFirst 静态实例优先（默认）：配了静态实例就短路注册中心。
	//
	// 保持既有行为不变：已按短路语义配置静态实例的调用方不受影响。
	FallbackStaticFirst FallbackOrder = iota

	// FallbackRegistryFirst 注册中心优先：先查注册中心，注册中心路径失败
	// 或返回空时才用静态实例兜底。
	//
	// 适用于「注册中心优先、静态地址兜底」的生产形态：注册中心有健康
	// 实例时不再被静态地址遮蔽（静态地址在实例换 IP 后不会跟着变），
	// 而注册中心冷启动、故障且缓存为空时仍有兜底可用。
	FallbackRegistryFirst
)

// String 返回降级序的可读名称。
func (o FallbackOrder) String() string {
	switch o {
	case FallbackRegistryFirst:
		return "registry-first"
	default:
		return "static-first"
	}
}

// WithFallbackOrder 设置全局降级序：静态实例与注册中心谁优先。
//
// 缺省为 FallbackStaticFirst（静态实例短路注册中心，与既有行为一致）。
// 设为 FallbackRegistryFirst 后，全部服务的静态实例降级为「兜底」：
// 注册中心路径失败或返回空时才参与取值。
//
// 单服务可用 WithStaticFallback 单独指定兜底语义，不受全局序影响。
func WithFallbackOrder(order FallbackOrder) ClientOption {
	return func(c *ServiceClient) { c.fallbackOrder = order }
}

// WithStaticFallback 配置指定服务的**兜底**静态实例。
//
// 与 WithStaticInstances 的区别只在参与时机：
//
//   - WithStaticInstances：短路静态——配了就直接返回，注册中心不被查询；
//   - WithStaticFallback：兜底静态——只有注册中心路径失败或返回空时才返回它。
//
// 典型用法是生产形态的「注册中心优先、静态地址兜底」：静态地址
// （如 k8s Service DNS）与注册中心同时配置，注册中心有健康实例时以
// 注册中心为准（静态地址在实例换 IP 后不会跟着变），注册中心冷启动或
// 故障时才用静态地址兜底。
//
// 同一服务上重复配置以最后一次调用为准：WithStaticFallback 与
// WithStaticInstances 写同一份静态实例表，前者把该服务标记为兜底语义。
func WithStaticFallback(serviceName string, instances []registry.Instance) ClientOption {
	return func(c *ServiceClient) {
		if c.staticInstances == nil {
			c.staticInstances = make(map[string][]registry.Instance)
		}
		c.staticInstances[serviceName] = instances
		if c.fallbackOnly == nil {
			c.fallbackOnly = make(map[string]bool)
		}
		c.fallbackOnly[serviceName] = true
	}
}

// staticShortCircuits 判断该服务的静态实例是否短路注册中心。
//
// 三种情况短路：未标记兜底语义、且全局序为静态优先、且确实配了静态实例。
func (c *ServiceClient) staticShortCircuits(serviceName string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.fallbackOnly[serviceName] {
		return false
	}
	if c.fallbackOrder != FallbackStaticFirst {
		return false
	}
	return len(c.staticInstances[serviceName]) > 0
}

// staticFallbackPool 返回该服务的兜底静态实例；不参与兜底时返回 nil。
//
// 兜底来源有两种：显式标记为兜底语义的服务（WithStaticFallback），
// 以及全局序为注册中心优先时的普通静态实例。
func (c *ServiceClient) staticFallbackPool(serviceName string) []registry.Instance {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.staticInstances[serviceName]) == 0 {
		return nil
	}
	if c.fallbackOnly[serviceName] || c.fallbackOrder == FallbackRegistryFirst {
		return copyInstances(c.staticInstances[serviceName])
	}
	return nil
}

// withStaticFallback 在注册中心路径没有给出实例时用兜底静态兜底。
//
// 触发条件与「注册中心优先」的语义一致：拿到空实例列表即视为注册中心
// 没有可用实例（冷启动、服务未注册、或实例全部不健康）。
func (c *ServiceClient) withStaticFallback(serviceName string, instances []registry.Instance) []registry.Instance {
	if len(instances) > 0 {
		return instances
	}
	if fb := c.staticFallbackPool(serviceName); len(fb) > 0 {
		return fb
	}
	return instances
}
