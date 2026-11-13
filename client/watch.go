package client

import (
	"context"
	"fmt"
	"slices"

	"github.com/oekit/logx"
	"github.com/oekit/registry"
)

// SubscribeService 订阅指定服务的实例变更，当实例列表变化时回调 callback。
//
// SDK 监听按服务去重：同一服务多次订阅只向 provider 注册一次监听，
// 推送时扇出到全部本地回调（否则 N 次订阅会导致每次推送触发 N² 次回调）。
// 去重路径（他人已注册监听）下，新回调会立即收到一次当前静态/缓存实例的
// 初始回调——SDK 推送是事件驱动的，若 provider 首推已过，否则新订阅方会
// "看似订阅成功却迟迟无回调"。Close 后调用返回 ErrClientClosed。
//
// 在途可见性（#173）：回调条目登记与在途槽（subscribing）同帧发布——
// 从条目落表起整个调用期间（登记、等待、去重初始回调、provider 注册）
// subscribing[service] 在位，UnsubscribeService 对**全部**在途订阅可等待，
// provider 侧「退订先落地、在途订阅随后复活监听」的幽灵订阅窗口闭合
// （此前槽在 ensureSubscribe 成为注册方时才登记，条目登记 → 槽登记
// 之间的在途订阅对退订不可见，退订可整段插队）。
//
// 回调收到的 []registry.Instance 为**只读快照**（#149）：推送路径是与
// provider 内存解耦的私有副本，初始回调是内部已发布快照的引用；两者均
// 不得修改（快照契约见 GetInstances godoc）。
func (c *ServiceClient) SubscribeService(ctx context.Context, serviceName string, callback func([]registry.Instance)) error {
	c.callbackMu.Lock()
	// Close 是终结操作：关闭后 SDK 客户端已 CloseClient，再向其注册监听
	// 会残留无效订阅（与 WatchService 的 closed 检查对称）。
	// closed 是 atomic（#96）：Close 持 c.mu 写、这里持 callbackMu 读——
	// 两把锁无 happens-before 边，普通 bool 读是数据竞争。
	if c.closed.Load() {
		c.callbackMu.Unlock()
		nnsLog.Warn("subscribe after close ignored", "service", serviceName)
		return ErrClientClosed
	}
	// 追加指针包装条目：Subscribe 失败时只按指针身份摘除自身——
	// 整体快照回滚在并发订阅同一服务时会把他方已成功注册的回调一并抹掉
	entry := &instanceCallback{fn: callback}
	// #173：在途槽与条目登记同帧发布。槽空闲时本调用持有它（其 done 在
	// 本调用收场时恰好一次关闭）；槽已被他方占用时本方是等待者（call 为
	// nil，等待期间不持有槽），后续在 ensureSubscribe 里按在途结果重新
	// 决策、接手时补登自己的槽。
	var call *subscribeCall
	if _, busy := c.subscribing[serviceName]; !busy {
		call = &subscribeCall{done: make(chan struct{})}
		if c.subscribing == nil {
			c.subscribing = make(map[string]*subscribeCall)
		}
		c.subscribing[serviceName] = call
	}
	// #165：扇出列表是 copy-on-write——追加也发布**全新切片**（full-slice
	// 表达式强制 append 重分配 backing array），已发布切片永不再被原地
	// 改写；推送扇出（newDispatch）在锁外遍历旧快照因此无数据竞争。
	old := c.instanceCallbacks[serviceName]
	c.instanceCallbacks[serviceName] = append(old[:len(old):len(old)], entry)
	c.callbackMu.Unlock()

	// Mock 模式或无订阅器时，使用静态配置或缓存
	if c.mockMode || c.subscriber == nil {
		instances := c.getInstancesFromStaticOrCache(serviceName)
		if len(instances) > 0 {
			safeInvoke("instance", serviceName, func() { callback(instances) })
		}
		// 无 provider 注册段，槽到调用收场为止（在途可见性口径与真实
		// 注册路径一致）
		c.releaseSubscribeSlot(serviceName, call)
		return nil
	}

	slot, registered, err := c.ensureSubscribe(ctx, serviceName, entry, call)
	if err != nil {
		c.releaseSubscribeSlot(serviceName, slot)
		return err
	}
	if !registered {
		// 去重路径：SDK 监听已由他方注册，推送只覆盖后续变更。
		// 补一次当前静态/缓存实例的初始回调，保证"订阅即有视图"。
		// 槽仍持有：#173 的在途可见性覆盖到初始回调收场——退订等待的
		// 是「整个在途订阅」，不只是 provider 注册段。
		instances := c.getInstancesFromStaticOrCache(serviceName)
		if len(instances) > 0 {
			safeInvoke("instance", serviceName, func() { callback(instances) })
		}
	}
	c.releaseSubscribeSlot(serviceName, slot)
	return nil
}

// ensureSubscribe 确保 serviceName 的 SDK 监听已注册。
//
// 返回值：slot 是本调用在返回时刻持有的在途槽（收场由 SubscribeService
// 统一 releaseSubscribeSlot——含去重路径的初始回调期，#173）；registered
// 为 true 表示「本次调用把监听注册上了」，false 表示「监听此前已注册」
// （调用方需自己补一次初始回调）。
//
// **注册是串行的**：同一服务同一时刻只有一个注册方在途（c.subscribing
// 记录在途槽），后到的并发订阅方等待在途注册的结果，而不是直接返回——
// 直接返回会让它在「前一个注册失败」时拿到 success 却永远收不到推送
// （永久失联）；等待则在失败后由它接手重试。
//
// 等待者醒来后**重新决策**（而不是照抄前一个的结论）：多个等待者同时醒来时
// 只有第一个能拿到注册资格，其余看到「已注册」走去重路径 ⇒ provider 只被
// 注册一次。
//
// 等待期间等待者自身的回调条目**保留在扇出列表里**（#104）：在途注册成功后
// 醒来的等待者走去重路径，条目必须在——摘了的话 SubscribeService 返回成功
// 但回调永不在扇出列表，实例变更全部丢失。只有 ctx 真正取消（#95）或注册方
// 失败（注册方自己的回滚）才摘条目。
//
// 在途槽的所有权（#173）：槽在 SubscribeService 里与条目登记同帧抢占
// （空闲时）；等待者 call==nil 不持有槽，接手注册时补登自己的槽并在返回值
// 里上交所有权。槽的 done 恰好一次关闭：持有者在收场（调用终态）或「让位」
// （等在途退订，见下）时关闭——等待者据此唤醒重新决策，UnsubscribeService
// 据此等整个在途订阅出结果。
func (c *ServiceClient) ensureSubscribe(ctx context.Context, serviceName string, entry *instanceCallback, call *subscribeCall) (*subscribeCall, bool, error) {
	for {
		c.callbackMu.Lock()
		// #152：Close 是终结操作——关闭后不再向 provider 发起新注册（否则
		// 监听注册落在 Close 的 Unsubscribe 之后 ⇒ 永久残留）。与
		// SubscribeService 入口的 closed 检查对称：入口检查挡住「关闭后
		// 才发起」的调用，这里挡住「发起时未关闭、等待/重入期间 Close
		// 已完成」的调用（等待在途注册/退订醒来的循环每轮都复查）。
		if c.closed.Load() {
			c.callbackMu.Unlock()
			c.rollbackEntry(serviceName, entry)
			return call, false, ErrClientClosed
		}
		// 监听已注册 ⇒ 走去重路径（复用，不重复注册）。
		if c.subscribedServices[serviceName] {
			c.callbackMu.Unlock()
			return call, false, nil
		}
		// 有在途退订 ⇒ 等它结束再决策（#147）：否则「本地已清、provider
		// Unsubscribe 还在途」的窗口里本方完成新订阅，那次退订会把这条
		// **新监听**一并拆除，而本地已置 subscribedServices ⇒ 永不重注册 ⇒
		// 实例变更推送全部丢失。等待后本地状态是干净的（未订阅），本方
		// 接手注册即恢复监听。
		//
		// #173：持有在途槽时先**让位**（摘除自己的槽并关闭 done）再等待
		// ——槽在位会挡住退订的「等在途订阅」步骤，而本方又在等退订 ⇒
		// 死锁环。让位后本方对退订不可见（此时本方没有待落地的 provider
		// 注册，退订无须等本方），由本方自己串行化到退订之后：醒来重
		// 决策时槽位空缺则补登自己的槽，provider 侧次序必然是
		// 退订落地 → 本方注册。原本停在本方 done 上的等待者同时被唤醒，
		// 落到同一处退订等待，醒来后重新决策。
		if ucall, ok := c.unsubscribing[serviceName]; ok {
			c.releaseSubscribeSlotLocked(serviceName, call)
			call = nil
			c.callbackMu.Unlock()
			select {
			case <-ucall.done:
				continue
			case <-ctx.Done():
				c.rollbackEntry(serviceName, entry)
				return nil, false, ctx.Err()
			}
		}
		// 有在途注册（他方的槽）⇒ 等它的结果，醒来后重新决策。本方持有的
		// 槽必然是 map 里那一个（他方只在槽空闲时登记），故 call != nil 时
		// 不会进入本分支。
		if inflight, ok := c.subscribing[serviceName]; ok && inflight != call {
			c.callbackMu.Unlock()
			select {
			case <-inflight.done:
				// 在途注册已出结果：自己的 entry **留在扇出列表里**（#104），
				// 回循环顶重新决策——注册成功则走去重路径（条目在，推送
				// 扇出可达本回调）；失败则本方接手重试。此前 #95 修复把
				// 摘除放在 select 之前无条件执行，成功醒来的等待者条目
				// 已被摘且无人补回——并发订阅同一服务时后到者「订阅成功
				// 但回调永不触发」。
				continue
			case <-ctx.Done():
				// 只有 ctx 真正取消时才按指针身份回滚自己刚追加的 entry
				// （#95）——与注册方失败路径的回滚同构；不摘的话注册一旦
				// 成功，已「失败返回」的回调会收到后续所有 SDK 推送且永不
				// 清理。（此刻在途注册可能恰好完成：照样摘，本调用方拿到
				// 的是错误，不该再收推送。）
				c.rollbackEntry(serviceName, entry)
				return call, false, ctx.Err()
			}
		}
		// 本次调用成为注册方：槽已在位（SubscribeService 同帧抢占），或
		// 空缺（#173 等待醒来让位后接手——补登自己的槽）。
		if call == nil {
			call = &subscribeCall{done: make(chan struct{})}
			c.subscribing[serviceName] = call
		}
		c.callbackMu.Unlock()

		err := c.subscriber.Subscribe(ctx, serviceName, c.newDispatch(serviceName))

		// 公布结果：**先在锁内改状态，再唤醒等待者**——等待者醒来后要能看到
		// 最终状态（已注册 / 未注册）才能正确决策；顺序反了它会读到中间态。
		// #173：槽的摘除不在 commit 做——收场统一在 SubscribeService 返回时
		// releaseSubscribeSlot（覆盖去重初始回调期），等待者按 done 关闭唤醒。
		compensate := false
		c.callbackMu.Lock()
		switch {
		case err != nil:
			// 回滚：只摘除本次注册的条目（并发订阅方不受影响），
			// 并把服务标记留在「未注册」⇒ 等待者接手重试。
			c.rollbackEntryLocked(serviceName, entry)
		case c.closed.Load():
			// #152：注册期间 Close 已跑完全部清理——这次 Subscribe 落在了
			// Close 的 Unsubscribe 之后（或 Close 的等待预算已耗尽），provider
			// 侧监听无人再注销 ⇒ 立即补偿注销；且不得复写 subscribedServices
			// （Close 已清空，重新置位会让已关闭的客户端被标回「已订阅」）。
			// 对齐 WatchService 注册成功后的 closed 复查补偿（config.go）。
			compensate = true
		default:
			if c.subscribedServices == nil {
				c.subscribedServices = make(map[string]bool)
			}
			c.subscribedServices[serviceName] = true
		}
		c.callbackMu.Unlock()

		if compensate {
			// 有界预算：Close 是终结操作，补偿注销不得无限期挂住
			// （provider 不响应时与 closeDefaultTimeout 同口径）。
			compensateCtx, cancel := context.WithTimeout(context.Background(), closeDefaultTimeout)
			if uerr := c.subscriber.Unsubscribe(compensateCtx, serviceName); uerr != nil {
				nnsLog.Warn("compensating unsubscribe after close failed", "service", serviceName, logx.ErrKey, uerr)
			}
			cancel()
		}

		if err != nil {
			return call, false, fmt.Errorf("subscribe service [%s]: %w", serviceName, err)
		}
		return call, true, nil
	}
}

// releaseSubscribeSlot 收场本调用持有的在途槽（#173）：身份核验摘除（map
// 里若已被他人接管则不动他方）+ close(done) 恰好一次。call==nil（本方是
// 等待者、从未持有槽或已在退订等待中让位）为 no-op。
func (c *ServiceClient) releaseSubscribeSlot(serviceName string, call *subscribeCall) {
	if call == nil {
		return
	}
	c.callbackMu.Lock()
	if cur, ok := c.subscribing[serviceName]; ok && cur == call {
		delete(c.subscribing, serviceName)
	}
	c.callbackMu.Unlock()
	close(call.done)
}

// releaseSubscribeSlotLocked 是 releaseSubscribeSlot 的锁内版本（调用方
// 必须已持有 callbackMu 写锁）：让位场景（等在途退订）在持锁状态下摘槽，
// 与状态决策同帧发布。
func (c *ServiceClient) releaseSubscribeSlotLocked(serviceName string, call *subscribeCall) {
	if call == nil {
		return
	}
	if cur, ok := c.subscribing[serviceName]; ok && cur == call {
		delete(c.subscribing, serviceName)
	}
	close(call.done)
}

// rollbackEntry 按指针身份把 entry 从扇出列表摘除（自带锁）。
func (c *ServiceClient) rollbackEntry(serviceName string, entry *instanceCallback) {
	c.callbackMu.Lock()
	defer c.callbackMu.Unlock()
	c.rollbackEntryLocked(serviceName, entry)
}

// rollbackEntryLocked 是 rollbackEntry 的锁内版本（调用方必须已持有
// callbackMu 写锁）。
//
// #165：摘除是 copy-on-write——不原地搬移（append(list[:i], list[i+1:]...)
// 会写共享 backing array，与锁外遍历旧快照的推送扇出构成数据竞争），
// 发布全新切片；锁外已取到旧快照的扇出方读旧数组（只读），天然无竞争。
func (c *ServiceClient) rollbackEntryLocked(serviceName string, entry *instanceCallback) {
	list := c.instanceCallbacks[serviceName]
	for i, e := range list {
		if e == entry {
			c.instanceCallbacks[serviceName] = slices.Concat(list[:i], list[i+1:])
			break
		}
	}
}

// newDispatch 返回该服务的 SDK 推送派发回调。
//
// 每次注册一个：provider 侧对同一服务重复注册会**覆盖**上一次的回调
// （nacos SDK 的行为），而本地扇出靠 instanceCallbacks 列表 ⇒ 回调只负责
// 把实例广播给列表里的所有订阅者。
func (c *ServiceClient) newDispatch(serviceName string) func([]registry.Instance) {
	return func(instances []registry.Instance) {
		// 发布缓存快照（updateCache 内部一次深拷贝，与 provider 推送的
		// 私有内存解耦——上游复用缓冲不再可能腐蚀缓存，#149）
		c.updateCache(serviceName, instances)

		// 派发快照：回调拿到的是拷贝（与 provider 内存解耦），避免 provider
		// 缓冲复用或单个回调修改影响其他回调
		dispatch := copyInstances(instances)

		c.callbackMu.RLock()
		// #165：扇出列表是 copy-on-write（追加/回滚都发布全新切片，见
		// SubscribeService / rollbackEntryLocked）——这里取到的切片头指向
		// 的 backing array 在发布后永不再被写，放锁后遍历旧快照无数据竞争。
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
//
// 并发纪律（#147/#173）：「清本地状态 + 标记退订中」与「provider 退订」
// 之间不是原子的，双向都在途等待交接——
//   - 退订方向（#173）：本方法先等**全部在途 SubscribeService 出结果**
//     （subscribing 槽的 done 关闭）再落地 provider 退订。在途槽与回调
//     条目同帧发布、覆盖订阅调用的全程（登记、等待、去重初始回调、
//     provider 注册），provider 侧次序必然是「在途订阅先落地 → 本退订」
//     ——否则退订插队落地后，在途订阅随后注册监听 ⇒ 已退订服务被
//     幽灵复活，退订谎报成功。已在途退订（unsubscribing 在位）时，
//     在途订阅的接手者会让位并自行串行化到本退订之后，无死锁环。
//   - 订阅方向（#147）：退订期间（unsubscribing 标记在位）并发
//     SubscribeService 会像等待在途注册一样等待退订落地后再注册新监听
//     ——否则本次 provider 退订会把窗口里建立的新监听一并拆除，而本地
//     已视为已订阅 ⇒ 永不重注册，实例变更推送全部丢失。
//
// 并发多次 Unsubscribe 收敛：后来者等待首次退订结束后按「未订阅」状态
// 重入（对 provider 幂等）。
func (c *ServiceClient) UnsubscribeService(ctx context.Context, serviceName string) error {
	for {
		c.callbackMu.Lock()
		// 等在途订阅出结果（#173：槽覆盖订阅调用全程）：否则「注册」与
		// 「注销」在 provider 侧的先后不可控——注册可能落在注销之后 ⇒
		// 监听残留/幽灵复活（本地标记已删，下次订阅会重复注册）。
		if call, ok := c.subscribing[serviceName]; ok {
			c.callbackMu.Unlock()
			select {
			case <-call.done:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		// 已有在途退订 ⇒ 等它结束（并发退订去重到单个 provider 调用）。
		if ucall, ok := c.unsubscribing[serviceName]; ok {
			c.callbackMu.Unlock()
			select {
			case <-ucall.done:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		ucall := &subscribeCall{done: make(chan struct{})}
		if c.unsubscribing == nil {
			c.unsubscribing = make(map[string]*subscribeCall)
		}
		c.unsubscribing[serviceName] = ucall
		delete(c.instanceCallbacks, serviceName)
		delete(c.subscribedServices, serviceName)
		c.callbackMu.Unlock()

		if c.mockMode || c.subscriber == nil {
			c.callbackMu.Lock()
			delete(c.unsubscribing, serviceName)
			c.callbackMu.Unlock()
			close(ucall.done)
			return nil
		}

		err := c.subscriber.Unsubscribe(ctx, serviceName)

		// 先摘标记再唤醒等待者：醒来者看到的是「退订已落地、本地干净」的
		// 终态，接手注册即恢复监听（#147）。
		c.callbackMu.Lock()
		delete(c.unsubscribing, serviceName)
		c.callbackMu.Unlock()
		close(ucall.done)
		return err
	}
}
