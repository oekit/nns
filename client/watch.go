package client

import (
	"context"
	"fmt"

	"github.com/oekit/registry"
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
	c.callbackMu.Unlock()

	// Mock 模式或无订阅器时，使用静态配置或缓存
	if c.mockMode || c.subscriber == nil {
		instances := c.getInstancesFromStaticOrCache(serviceName)
		if len(instances) > 0 {
			safeInvoke("instance", serviceName, func() { callback(instances) })
		}
		return nil
	}

	registered, err := c.ensureSubscribe(ctx, serviceName, entry)
	if err != nil {
		return err
	}
	if !registered {
		// 去重路径：SDK 监听已由他方注册，推送只覆盖后续变更。
		// 补一次当前静态/缓存实例的初始回调，保证"订阅即有视图"。
		instances := c.getInstancesFromStaticOrCache(serviceName)
		if len(instances) > 0 {
			safeInvoke("instance", serviceName, func() { callback(instances) })
		}
	}
	return nil
}

// ensureSubscribe 确保 serviceName 的 SDK 监听已注册。
//
// 返回 true 表示「本次调用把监听注册上了」，false 表示「监听此前已注册」
// （调用方需自己补一次初始回调）。
//
// **注册是串行的**：同一服务同一时刻只有一个注册方在途（c.subscribing 记录
// 在途状态），后到的并发订阅方等待在途注册的结果，而不是直接返回——
// 直接返回会让它在「前一个注册失败」时拿到 success 却永远收不到推送
// （永久失联）；等待则在失败后由它接手重试。
//
// 等待者醒来后**重新决策**（而不是照抄前一个的结论）：多个等待者同时醒来时
// 只有第一个能拿到注册资格，其余看到「已注册」走去重路径 ⇒ provider 只被
// 注册一次。
func (c *ServiceClient) ensureSubscribe(ctx context.Context, serviceName string, entry *instanceCallback) (bool, error) {
	for {
		c.callbackMu.Lock()
		// 监听已注册 ⇒ 走去重路径（复用，不重复注册）。
		if c.subscribedServices[serviceName] {
			c.callbackMu.Unlock()
			return false, nil
		}
		// 有在途注册 ⇒ 等它的结果，醒来后重新决策。
		if call, ok := c.subscribing[serviceName]; ok {
			c.callbackMu.Unlock()
			select {
			case <-call.done:
				continue
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
		// 本次调用成为注册方。
		call := &subscribeCall{done: make(chan struct{})}
		if c.subscribing == nil {
			c.subscribing = make(map[string]*subscribeCall)
		}
		if c.subscribedServices == nil {
			c.subscribedServices = make(map[string]bool)
		}
		c.subscribing[serviceName] = call
		c.callbackMu.Unlock()

		err := c.subscriber.Subscribe(ctx, serviceName, c.newDispatch(serviceName))

		// 公布结果：**先在锁内改状态，再唤醒等待者**——等待者醒来后要能看到
		// 最终状态（已注册 / 未注册）才能正确决策；顺序反了它会读到中间态。
		c.callbackMu.Lock()
		delete(c.subscribing, serviceName)
		if err != nil {
			// 回滚：只摘除本次注册的条目（并发订阅方不受影响），
			// 并把服务标记留在「未注册」⇒ 等待者接手重试。
			list := c.instanceCallbacks[serviceName]
			for i, e := range list {
				if e == entry {
					c.instanceCallbacks[serviceName] = append(list[:i], list[i+1:]...)
					break
				}
			}
		} else {
			c.subscribedServices[serviceName] = true
		}
		c.callbackMu.Unlock()
		close(call.done)

		if err != nil {
			return false, fmt.Errorf("subscribe service [%s]: %w", serviceName, err)
		}
		return true, nil
	}
}

// newDispatch 返回该服务的 SDK 推送派发回调。
//
// 每次注册一个：provider 侧对同一服务重复注册会**覆盖**上一次的回调
// （nacos SDK 的行为），而本地扇出靠 instanceCallbacks 列表 ⇒ 回调只负责
// 把实例广播给列表里的所有订阅者。
func (c *ServiceClient) newDispatch(serviceName string) func([]registry.Instance) {
	return func(instances []registry.Instance) {
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
	}
}

// UnsubscribeService 取消订阅指定服务的实例变更。
// 本地回调列表同步清除，避免再次 Subscribe 后旧回调重复触发。
// 注意：取消针对整个服务生效（所有调用方的回调一并清除），
// SDK 监听同时注销，下次 Subscribe 会重新注册。
func (c *ServiceClient) UnsubscribeService(ctx context.Context, serviceName string) error {
	// 等在途注册结束：否则「注册」与「注销」在 provider 侧的先后不可控——
	// 注册可能落在注销之后 ⇒ 监听残留（本地标记已删，下次订阅会重复注册）。
	c.callbackMu.Lock()
	call := c.subscribing[serviceName]
	c.callbackMu.Unlock()
	if call != nil {
		select {
		case <-call.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	c.callbackMu.Lock()
	delete(c.instanceCallbacks, serviceName)
	delete(c.subscribedServices, serviceName)
	c.callbackMu.Unlock()

	if c.mockMode || c.subscriber == nil {
		return nil
	}
	return c.subscriber.Unsubscribe(ctx, serviceName)
}
