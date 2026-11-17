package client

import (
	"context"
	"sync"
)

// 本文件是实例订阅（watch.go）与配置监听（config.go）两套**同构的在途握手
// 状态机**的单源机械件（nns#227）：键 + call{done channel} 槽结构、槽表
// 登记（含懒初始化）、身份核验摘除、「err 先于 close(done) 公布」协议
// （#211）、done/ctx 二选一等待原语、等待/接手/让位循环的让位等待、双向
// 等待的退订循环。两侧各自实例化一台（subscribeInflight / watchInflight），
// 语义差异全部留在两侧代码与注入回调（unregisterHooks），共享类型内不分叉。
//
// 协议不变量（#147/#152/#170/#173/#211 逐条保持）：
//   - 同一键同一方向至多一个在途槽；后到者等待在途结果后**重新决策**
//     （读状态位/标记），而不是照抄前一个的结论——成功复用、失败接手重试。
//   - 身份核验摘除：只摘自己登记的槽，map 里已被他人接管则不动他方。
//   - 「先锁内改状态（标记/槽表），锁外 close(done) 唤醒等待者」——等待者
//     醒来看到终态。唯一例外是让位路径（releaseLocked）：close 与循环内
//     状态决策同帧发布，见其 doc。
//   - 「err 先于 close(done) 公布」（#211）：channel 关闭建立 happens-before，
//     等待者在 <-done 之后读取 err 无数据竞争。

// subscribeCall 是一次「向 provider 落地/取消监听」的在途握手槽
// （nns#227 起槽表机械件单源到本文件的 inflightMachine）。
//
// done 关闭即表示该次握手**已有结果**（成功或失败）；等待者醒来后重新决策
// （读 subscribedServices / watching 等状态），而不是直接照抄结论。
//
// err 承载该次握手的结果（#211）：登记方在 close(done) **之前**写入，
// 等待者在 <-done 之后读取——channel 关闭建立 happens-before，无数据竞争。
// 仅配置监听注册路径（watchRegister → WatchService 等待者）读写；实例订阅
// /退订在途槽不用该字段（其结果经状态位重新决策）。
type subscribeCall struct {
	done chan struct{}
	err  error
}

// inflightDirection 在途握手的方向：注册（正向）与退订（反向）。
type inflightDirection int

const (
	// inflightForward 注册方向：实例订阅侧对应 subscribing、配置监听侧对应
	// watching（向 provider 落地监听的在途握手）。
	inflightForward inflightDirection = iota
	// inflightReverse 退订方向：实例订阅侧对应 unsubscribing、配置监听侧
	// 对应 unwatching（向 provider 取消监听的在途握手）。
	inflightReverse
)

// inflightMachine 在途握手槽状态机（nns#227 单源）。
//
// 一台机器管一个业务面的两个方向：实例订阅侧（callbackMu 守护，
// subscribing/unsubscribing）与配置监听侧（c.mu 守护，watching/unwatching）。
// 机器本体只持有守护锁指针与两张槽表指针——三指针的轻量值，经
// ServiceClient.subscribeInflight / watchInflight **现取现用**：client 可能
// 绕过 New 直接构造（测试 newTestClient 同款路径），机器不做任何构造期绑定；
// 槽表本体仍是 ServiceClient 的命名字段（Close 快照与测试按字段访问），
// 指针间接一层仅为槽表懒初始化所需（*t = make(...) 写回字段）。
//
// 所有方法分两类：*Locked 后缀要求调用方已持 m.mu 写锁（循环顶决策帧内
// 使用）；其余自带锁。等待（awaitSlot）一律**不持锁**——等待者醒来后要拿
// 同一把锁重新决策，持锁等待会死锁。
type inflightMachine struct {
	mu      *sync.RWMutex              // 守护锁（两侧各自的锁，由取用方法绑定）
	forward *map[string]*subscribeCall // 注册方向在途槽表
	reverse *map[string]*subscribeCall // 退订方向在途槽表
}

// table 返回方向对应的槽表。
func (m inflightMachine) table(d inflightDirection) *map[string]*subscribeCall {
	if d == inflightForward {
		return m.forward
	}
	return m.reverse
}

// inflightLocked 取指定方向、指定键的在途槽（调用方已持 m.mu 写锁——循环
// 顶决策帧）。
func (m inflightMachine) inflightLocked(d inflightDirection, key string) (*subscribeCall, bool) {
	call, ok := (*m.table(d))[key]
	return call, ok
}

// claimLocked 无条件登记本方在途槽并懒初始化槽表：等待者接手注册时补登
// 自己的槽（#173）、退订登记本方退订槽。返回本方槽（done 由本方在收场时
// 恰好一次关闭）。
func (m inflightMachine) claimLocked(d inflightDirection, key string) *subscribeCall {
	t := m.table(d)
	if *t == nil {
		*t = make(map[string]*subscribeCall)
	}
	call := &subscribeCall{done: make(chan struct{})}
	(*t)[key] = call
	return call
}

// claimIfFreeLocked 注册入口的同帧抢占（#173 在途可见性）：槽空闲则登记并
// 返回本方槽（true）；已被他方占用则本方是等待者（nil, false——等待期间不
// 持有槽）。调用方已持 m.mu 写锁。
func (m inflightMachine) claimIfFreeLocked(d inflightDirection, key string) (*subscribeCall, bool) {
	if _, busy := (*m.table(d))[key]; busy {
		return nil, false
	}
	return m.claimLocked(d, key), true
}

// deleteLocked 按身份核验摘除槽（调用方已持 m.mu 写锁）：map 里若已被他人
// 接管（键上已换成本方之外的槽）则不动他方。
func (m inflightMachine) deleteLocked(d inflightDirection, key string, call *subscribeCall) {
	t := m.table(d)
	if cur, ok := (*t)[key]; ok && cur == call {
		delete(*t, key)
	}
}

// releaseLocked 锁内收场本方槽：身份核验摘除 + close(done)（**锁内**——
// 仅供让位路径使用：与循环内状态决策同帧发布，#173；其余收场一律走
// release 的「解锁后 close」时序）。call==nil（本方是等待者、从未持有槽或
// 已让位）为 no-op。
func (m inflightMachine) releaseLocked(d inflightDirection, key string, call *subscribeCall) {
	if call == nil {
		return
	}
	m.deleteLocked(d, key, call)
	close(call.done)
}

// release 收场本调用持有的槽：锁内身份核验摘除（map 里若已被他人接管则
// 不动他方）+ **解锁后** close(done) 恰好一次——先摘标记再唤醒等待者，
// 等待者醒来看到「已出结果、槽已收场」的终态。call==nil 为 no-op。
func (m inflightMachine) release(d inflightDirection, key string, call *subscribeCall) {
	if call == nil {
		return
	}
	m.mu.Lock()
	m.deleteLocked(d, key, call)
	m.mu.Unlock()
	close(call.done)
}

// fail 以「err 先于 close(done) 公布」的次序收场一次失败的握手（#211）：
// err 写入槽（等待中的调用方据此上抛；<-done 之后读取——channel 关闭建立
// happens-before，无数据竞争），随后按 release 的收场时序核验摘槽 + 唤醒
// 等待者。
func (m inflightMachine) fail(d inflightDirection, key string, call *subscribeCall, err error) {
	call.err = err
	m.release(d, key, call)
}

// awaitSlot 等待原语（两侧 8 处逐字近似的 select 块单源）：call.done 关闭
// 返回 nil——该次在途握手已出结果（成功或失败），等待者回循环顶重新决策；
// ctx 取消返回 ctx.Err()。不持锁调用。
func awaitSlot(ctx context.Context, call *subscribeCall) error {
	select {
	case <-call.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// unregisterHooks 双向等待退订循环（runUnregister）的差异注入：实例订阅与
// 配置监听的退订循环协议完全同构（#147/#170/#173），差异只在本地状态的
// 形状与 provider 入口——全部走钩子，共享类型内不分叉。
type unregisterHooks struct {
	// unmarked 幂等短路（锁内、两条在途等待之后）：本地无登记（去重标记
	// 不在位）返回 true ⇒ 直接 no-op nil（配置侧「未监听过的服务」）；实例
	// 订阅侧为 nil——即便本地无标记也走完整退订帧（清扇出列表与已订阅
	// 标记），对 provider 幂等。
	unmarked func(key string) bool
	// onClaimed 本方退订槽登记同帧的本地清理（锁内）：订阅侧摘扇出列表与
	// 已订阅标记（退订针对整个服务生效，再次 Subscribe 后旧回调不得重复
	// 触发）；配置侧摘监听去重标记。
	onClaimed func(key string)
	// invoke provider 退订调用（锁外）：mock / 未注入 provider 的短路在
	// 钩子内直接返回 nil——本地状态已在登记帧清理完毕，照常走收场帧摘槽
	// 唤醒。错误在钩子内完成包装（配置侧点名服务；订阅侧原样上抛）。
	invoke func(ctx context.Context, key string) error
	// onFailure 退订失败收尾（锁外、返回错误前）：配置侧条件回滚去重标记
	// （允许重试，不覆盖他人的新标记）；实例订阅侧为 nil（本地已清、错误
	// 上抛即可——下次 Subscribe 重新注册）。
	onFailure func(key string)
}

// runUnregister 双向等待退订循环（nns#227 单源；#147/#170/#173）。
//
// 并发纪律：「清本地状态 + 标记退订中」与「provider 退订」之间不是原子的，
// 双向都在途等待交接——
//   - 退订方向（#173 实例侧 / #170 配置侧）：先等**全部正向在途握手出结果**
//     （forward 槽的 done 关闭）再落地 provider 退订——实例侧在途槽与回调
//     条目同帧发布、覆盖订阅调用全程（登记、等待、去重初始回调、provider
//     注册），配置侧在途槽与去重标记同帧发布、覆盖注册落地全程；provider
//     侧次序必然是「在途握手先落地 → 本退订」——否则退订插队落地后，在途
//     握手随后落地 ⇒ 已退订服务被幽灵复活，退订谎报成功。
//   - 注册方向（#147 / #170）：退订期间（reverse 槽在位）并发的注册方向
//     调用会像等待在途注册一样等待退订落地后再登记——否则本次 provider
//     退订会把窗口里建立的新监听一并拆除，而本地已视为已登记 ⇒ 永不重
//     注册，推送全部丢失。
//
// 并发多次退订收敛：后来者等首次退订结束后按「未登记」状态重入（对
// provider 幂等）。
//
// 收场次序：锁内核验摘槽 → 解锁 → close(done)——等待者醒来看到「退订已出
// 结果、本地干净」的终态，接手注册即恢复监听（#147）。身份核验摘除在所有
// 可达交错下都命中本方槽：并发重新登记必须先等本槽 done 关闭（见上方等待
// 分支），而 done 在摘槽之后才关闭（实例订阅侧旧实现的无条件删除在此约束
// 下与之等价，收编统一为核验版）。
func (m inflightMachine) runUnregister(ctx context.Context, key string, hks unregisterHooks) error {
	for {
		m.mu.Lock()
		// 等正向在途握手出结果：否则「注册」与「注销」在 provider 侧的
		// 先后不可控——注册可能落在注销之后 ⇒ 监听残留/幽灵复活（本地
		// 标记已删，下次订阅会重复注册）。
		if call, ok := m.inflightLocked(inflightForward, key); ok {
			m.mu.Unlock()
			if err := awaitSlot(ctx, call); err != nil {
				return err
			}
			continue
		}
		// 已有在途退订 ⇒ 等它结束（并发退订去重到单个 provider 调用）。
		if ucall, ok := m.inflightLocked(inflightReverse, key); ok {
			m.mu.Unlock()
			if err := awaitSlot(ctx, ucall); err != nil {
				return err
			}
			continue
		}
		// 本地无登记 ⇒ 幂等 no-op（配置侧注入；实例订阅侧无此步）。
		if hks.unmarked != nil && hks.unmarked(key) {
			m.mu.Unlock()
			return nil
		}
		// 登记本方退订槽 + 清本地状态（同帧发布）。
		ucall := m.claimLocked(inflightReverse, key)
		if hks.onClaimed != nil {
			hks.onClaimed(key)
		}
		m.mu.Unlock()

		err := hks.invoke(ctx, key)

		// 先摘标记再唤醒等待者：醒来者看到的是「退订已出结果、本地干净」
		// 的终态，接手注册即恢复监听（#147）。
		m.mu.Lock()
		m.deleteLocked(inflightReverse, key, ucall)
		m.mu.Unlock()
		close(ucall.done)

		if err != nil {
			// 失败收尾（配置侧条件回滚去重标记，允许重试退订；不覆盖他人
			// 的新标记——唤醒的并发注册可能在回滚前已重新登记）。
			if hks.onFailure != nil {
				hks.onFailure(key)
			}
			return err
		}
		return nil
	}
}

// subscribeInflight 返回实例订阅侧的在途握手状态机：callbackMu 守护，
// subscribing（注册方向）/ unsubscribing（退订方向）两向槽表。
func (c *ServiceClient) subscribeInflight() inflightMachine {
	return inflightMachine{
		mu:      &c.callbackMu,
		forward: &c.subscribing,
		reverse: &c.unsubscribing,
	}
}

// watchInflight 返回配置监听侧的在途握手状态机：c.mu 守护，
// watching（注册方向）/ unwatching（退订方向）两向槽表。
func (c *ServiceClient) watchInflight() inflightMachine {
	return inflightMachine{
		mu:      &c.mu,
		forward: &c.watching,
		reverse: &c.unwatching,
	}
}
