package client

import (
	"context"
	"fmt"
	"github.com/yi-nology/ekit-registry/registry"
)

// SubscribeService 订阅指定服务的实例变更，当实例列表变化时回调 callback。
//
// SDK 监听按服务去重：同一服务多次订阅只向 provider 注册一次监听，
// 推送时扇出到全部本地回调（否则 N 次订阅会导致每次推送触发 N² 次回调）。
// 去重路径（他人已注册监听）下，新回调会立即收到一次当前静态/缓存实例的
// 初始回调——SDK 推送是事件驱动的，若 provider 首推已过，否则新订阅方会
// "看似订阅成功却迟迟无回调"。Close 后调用返回 ErrClientClosed。
func (c *ServiceClient) SubscribeService(ctx context.Context, serviceName string, callback func([]registry.Instance)) error {
	c.callbackMu.Lock()
	// Close 是终结操作：关闭后 SDK 客户端已 CloseClient，再向其注册监听
	// 会残留无效订阅（与 WatchService 的 closed 检查对称）
	if c.closed {
		c.callbackMu.Unlock()
		nnsLog.Warn("subscribe after close ignored", "service", serviceName)
		return ErrClientClosed
	}
	// 追加指针包装条目：Subscribe 失败时只按指针身份摘除自身——
	// 整体快照回滚在并发订阅同一服务时会把他方已成功注册的回调一并抹掉
	entry := &instanceCallback{fn: callback}
	c.instanceCallbacks[serviceName] = append(c.instanceCallbacks[serviceName], entry)
	needSubscribe := !c.mockMode && c.subscriber != nil && !c.subscribedServices[serviceName]
	if needSubscribe {
		if c.subscribedServices == nil {
			c.subscribedServices = make(map[string]bool)
		}
		c.subscribedServices[serviceName] = true
	}
	c.callbackMu.Unlock()

	// Mock 模式或无订阅器时，使用静态配置或缓存
	if c.mockMode || c.subscriber == nil {
		instances := c.getInstancesFromStaticOrCache(serviceName)
		if len(instances) > 0 {
			safeInvoke("instance", serviceName, func() { callback(instances) })
		}
		return nil
	}

	if !needSubscribe {
		// 去重路径：SDK 监听已由首个订阅方注册，推送只覆盖后续变更。
		// 补一次当前静态/缓存实例的初始回调，保证"订阅即有视图"。
		instances := c.getInstancesFromStaticOrCache(serviceName)
		if len(instances) > 0 {
			safeInvoke("instance", serviceName, func() { callback(instances) })
		}
		return nil
	}

	if needSubscribe {
		err := c.subscriber.Subscribe(ctx, serviceName, func(instances []registry.Instance) {
			// 更新本地缓存（缓存保留原始切片，读取时已深拷贝）
			c.updateCache(serviceName, instances)

			// 派发快照：回调拿到的是拷贝，避免单个回调修改污染其他回调与缓存
			dispatch := copyInstances(instances)

			c.callbackMu.RLock()
			callbacks := c.instanceCallbacks[serviceName]
			c.callbackMu.RUnlock()

			for _, cb := range callbacks {
				safeInvoke("instance", serviceName, func() { cb.fn(dispatch) })
			}
		})
		if err != nil {
			// 回滚：只摘除本次注册的条目与去重标记（并发订阅方不受影响）
			c.callbackMu.Lock()
			list := c.instanceCallbacks[serviceName]
			for i, e := range list {
				if e == entry {
					c.instanceCallbacks[serviceName] = append(list[:i], list[i+1:]...)
					break
				}
			}
			delete(c.subscribedServices, serviceName)
			c.callbackMu.Unlock()
			return fmt.Errorf("subscribe service [%s]: %w", serviceName, err)
		}
	}

	return nil
}

// UnsubscribeService 取消订阅指定服务的实例变更。
// 本地回调列表同步清除，避免再次 Subscribe 后旧回调重复触发。
// 注意：取消针对整个服务生效（所有调用方的回调一并清除），
// SDK 监听同时注销，下次 Subscribe 会重新注册。
func (c *ServiceClient) UnsubscribeService(ctx context.Context, serviceName string) error {
	c.callbackMu.Lock()
	delete(c.instanceCallbacks, serviceName)
	delete(c.subscribedServices, serviceName)
	c.callbackMu.Unlock()

	if c.mockMode || c.subscriber == nil {
		return nil
	}
	return c.subscriber.Unsubscribe(ctx, serviceName)
}
