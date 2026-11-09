package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/registry"
)

// gateSubscriber 是「注册可控」的订阅器：Subscribe 会阻塞到 gate 放行，
// 且可以配置为失败。lastCB 记录最后一次注册的派发回调，供测试在注册
// 成功后模拟 provider 推送（#104 复现需要）。
type gateSubscriber struct {
	mu       sync.Mutex
	gate     chan struct{}
	failWith error
	calls    int
	entered  chan struct{}
	once     sync.Once
	lastCB   func([]registry.Instance)
}

func newGateSubscriber() *gateSubscriber {
	return &gateSubscriber{gate: make(chan struct{}), entered: make(chan struct{})}
}

func (s *gateSubscriber) Subscribe(ctx context.Context, service string, cb func([]registry.Instance)) error {
	s.mu.Lock()
	s.calls++
	s.lastCB = cb
	s.mu.Unlock()
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	err := s.failWith
	s.mu.Unlock()
	return err
}

func (s *gateSubscriber) Unsubscribe(ctx context.Context, service string) error { return nil }

// TestSubscribeService_ConcurrentFirstFails_SecondMustNotBeStranded 回归测试：
// 第一个订阅方的 provider 注册**失败**时，并发的第二个订阅方不得「拿到 success
// 却永远收不到推送」。
//
// 缺陷成因：subscribedServices 在调用 provider **之前**就置位 ⇒ 后到的并发
// 订阅方看到它已置位 ⇒ 走去重路径直接返回 success。可前一个的注册随后失败并
// 回滚 ⇒ 后一个以为订阅成功、实际没人注册监听 ⇒ 它的回调永远不会再被触发
// （永久失联）。
//
// 修法：把「注册中」与「已注册」拆成两个状态；后到者等待在途注册的结果，
// 前一个失败则由它接手重试。
func TestSubscribeService_ConcurrentFirstFails_SecondMustNotBeStranded(t *testing.T) {
	sub := newGateSubscriber()
	sub.failWith = errors.New("provider down")
	c := newTestClient(t, WithSubscriber(sub))

	ctx := context.Background()
	firstErr := make(chan error, 1)
	go func() {
		firstErr <- c.SubscribeService(ctx, "svc", func([]registry.Instance) {})
	}()

	// 等第一个订阅方进入 provider 注册（阻塞在 gate 上）。
	select {
	case <-sub.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("第一个订阅方未进入 provider 注册")
	}

	// 第二个订阅方并发进入：它必须等在途注册的结果，而不是直接返回 success。
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- c.SubscribeService(ctx, "svc", func([]registry.Instance) {})
	}()

	// 第二个订阅方此刻**不应**已经返回（注册还没出结果）。
	select {
	case err := <-secondDone:
		t.Fatalf("第二个订阅方在注册结果出来前就返回了（err=%v）——它没等在途注册", err)
	case <-time.After(200 * time.Millisecond):
	}

	// 放行注册：第一个失败。
	close(sub.gate)

	if err := <-firstErr; err == nil {
		t.Fatal("第一个订阅方应收到注册失败")
	}

	// 第二个订阅方：前一个失败 ⇒ 它必须**接手重试**，而不是返回「成功但不订阅」。
	select {
	case err := <-secondDone:
		if err == nil {
			t.Fatal("第二个订阅方返回 success，但它并未真正订阅（provider 注册失败）⇒ 永久失联")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("第二个订阅方一直未返回")
	}
}

// TestSubscribeService_ConcurrentFirstFails_SecondReuses 回归测试：
// 第一个注册失败后，**多个**等待者被唤醒时只能有**一个**接手注册，
// 其余走去重路径 —— 否则 provider 侧被注册多次 ⇒ 同一服务多份监听
// ⇒ 每次推送触发多轮回调。
//
// 缺陷成因（朴素修法引入）：等待者醒来后各自「接手重试」，没有重新检查
// 是否已有别人注册成功 ⇒ N 个等待者 = N 次注册。
func TestSubscribeService_ConcurrentFirstFails_SecondReuses(t *testing.T) {
	sub := newFlakySubscriber()
	c := newTestClient(t, WithSubscriber(sub))

	ctx := context.Background()
	const waiters = 4
	firstErr := make(chan error, 1)
	go func() {
		firstErr <- c.SubscribeService(ctx, "svc", func([]registry.Instance) {})
	}()

	// 等第一个订阅方进入 provider 注册（阻塞在 gate 上）。
	select {
	case <-sub.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("第一个订阅方未进入 provider 注册")
	}

	// 多个等待者并发进入：它们必须等在途注册的结果。
	errs := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			errs <- c.SubscribeService(ctx, "svc", func([]registry.Instance) {})
		}()
	}

	// 等待者此刻都还不该返回（注册还没出结果）。
	select {
	case err := <-errs:
		t.Fatalf("等待者在注册结果出来前就返回了（err=%v）", err)
	case <-time.After(200 * time.Millisecond):
	}

	// 放行注册：第一次失败，之后成功。
	close(sub.gate)

	if err := <-firstErr; err == nil {
		t.Fatal("第一个订阅方应收到注册失败")
	}
	for i := 0; i < waiters; i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("等待者 %d 失败：%v", i, err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("等待者 %d 一直未返回", i)
		}
	}

	// provider 只被注册 2 次：1 次失败 + 1 次接手成功。
	sub.mu.Lock()
	calls := sub.calls
	sub.mu.Unlock()
	if calls != 2 {
		t.Fatalf("provider 被注册 %d 次，期望 2 次（1 次失败 + 1 次接手），多等待者不得各自注册", calls)
	}
}

// flakySubscriber 第一次 Subscribe 失败，之后成功；用于验证「失败的注册被
// 接手后不会引发重复注册」。
type flakySubscriber struct {
	mu      sync.Mutex
	calls   int
	gate    chan struct{}
	entered chan struct{}
	once    sync.Once
}

func newFlakySubscriber() *flakySubscriber {
	return &flakySubscriber{gate: make(chan struct{}), entered: make(chan struct{})}
}

func (s *flakySubscriber) Subscribe(ctx context.Context, service string, cb func([]registry.Instance)) error {
	s.mu.Lock()
	s.calls++
	calls := s.calls
	s.mu.Unlock()
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	if calls == 1 {
		return errors.New("provider down")
	}
	return nil
}

func (s *flakySubscriber) Unsubscribe(ctx context.Context, service string) error { return nil }

// TestSubscribeService_ConcurrentFirstSucceeds_SecondReuses 回归护栏：第一个
// 注册**成功**时，后到的订阅方复用监听（provider 只被注册一次）、且拿到
// 初始回调。
func TestSubscribeService_ConcurrentFirstSucceeds_SecondReuses(t *testing.T) {
	sub := newGateSubscriber()
	c := newTestClient(t, WithSubscriber(sub))

	ctx := context.Background()
	first := make(chan error, 1)
	go func() {
		first <- c.SubscribeService(ctx, "svc", func([]registry.Instance) {})
	}()
	select {
	case <-sub.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("第一个订阅方未进入 provider 注册")
	}

	second := make(chan error, 1)
	go func() {
		second <- c.SubscribeService(ctx, "svc", func([]registry.Instance) {})
	}()
	close(sub.gate)

	if err := <-first; err != nil {
		t.Fatalf("第一个订阅方失败：%v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("第二个订阅方失败：%v", err)
	}
	sub.mu.Lock()
	calls := sub.calls
	sub.mu.Unlock()
	if calls != 1 {
		t.Fatalf("provider 被注册 %d 次，期望 1 次（并发订阅应去重）", calls)
	}
}

// #96：并发 Close × Subscribe——closed 是 atomic（原 bool 两把锁无
// happens-before 边，-race 实锤）；本用例在 -race 下是回归钉。
func TestSubscribe_ConcurrentClose_NoRace(t *testing.T) {
	c := newTestClient(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_ = c.SubscribeService(ctx, "svc-race", func([]registry.Instance) {})
		}()
	}
	wg.Add(1)
	go func() { defer wg.Done(); _ = c.CloseWithContext(context.Background()) }()
	wg.Wait()
}

// leakSubscriber：Subscribe 阻塞到 release 后以成功收场（真实派发一次推送）。
type leakSubscriber struct {
	release chan struct{}
}

func (l *leakSubscriber) Subscribe(ctx context.Context, service string, cb func([]registry.Instance)) error {
	<-l.release
	cb(nil)
	return nil
}

func (l *leakSubscriber) Unsubscribe(ctx context.Context, service string) error { return nil }

// #95：等待在途注册时 ctx 超时返回——回调条目必须被摘除，注册成功后
// 不再收到推送（泄漏回归钉）。
func TestSubscribe_CtxDeadlineDuringPending_NoCallbackLeak(t *testing.T) {
	sub := &leakSubscriber{release: make(chan struct{})}
	c := newTestClient(t, WithSubscriber(sub))

	pushes := make(chan int, 4)
	leaked := make(chan int, 4)
	firstErr := make(chan error, 1)
	go func() {
		firstErr <- c.SubscribeService(context.Background(), "svc-leak", func(is []registry.Instance) {
			pushes <- len(is)
		})
	}()
	// 等第一个成为在途注册方（阻塞在 sub.Subscribe）
	time.Sleep(150 * time.Millisecond)

	// 第二个订阅方用超时 ctx，等到 ctx 过期
	ctx2, cancel2 := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel2()
	if err2 := c.SubscribeService(ctx2, "svc-leak", func(is []registry.Instance) {
		leaked <- len(is) // 「已失败返回」的回调若条目未摘除，会从这里漏进来
	}); err2 == nil {
		t.Fatal("第二个订阅方应因 ctx 超时返回错误")
	}

	// 放行第一个注册成功（其派发会推送一次）
	close(sub.release)
	if err := <-firstErr; err != nil {
		t.Fatalf("first subscribe: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// 泄漏断言：摘除后的回调不得再收到推送（第一个回调的 pushes 合法收一次）
	select {
	case <-leaked:
		t.Fatal("#95 回归：已「失败返回」的回调仍收到注册成功后的推送——条目未摘除")
	case <-pushes:
	}
	select {
	case <-leaked:
		t.Fatal("#95 回归：泄漏条目收到推送")
	default:
	}
}

// TestSubscribeService_ConcurrentWaiterKeepsCallbackEntry 回归 nns#104：
// 等待在途注册的订阅方，在注册**成功**醒来时自身回调条目必须仍在扇出列表
// 里。此前 #95 修复把「按指针身份摘除自身 entry」放在 select **之前**无条件
// 执行——与 ctx 是否取消无关：call.done 醒来（ctx 未取消）走去重路径时，
// entry 已被摘且无人补回 ⇒ SubscribeService 返回 nil（成功）但回调一次都不
// 触发，此后该服务的实例上下线推送全部丢失，服务发现核心契约
// 「订阅成功 ⇒ 一定收到变更推送」被静默违背。
//
// 修法：摘除移进 ctx.Done 分支——只有 ctx 真正取消才回滚自身条目（#95
// 语义保留），成功醒来者条目保留。
func TestSubscribeService_ConcurrentWaiterKeepsCallbackEntry(t *testing.T) {
	sub := newGateSubscriber()
	c := newTestClient(t, WithSubscriber(sub))

	var aHits, bHits int32
	ctx := context.Background()

	// A 先订阅：成为注册方，卡在 provider 的 Subscribe 上（gate 未放行）
	firstErr := make(chan error, 1)
	go func() {
		firstErr <- c.SubscribeService(ctx, "svc", func([]registry.Instance) {
			atomic.AddInt32(&aHits, 1)
		})
	}()
	select {
	case <-sub.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("第一个订阅方未进入 provider 注册")
	}

	// B 并发订阅：A 在途（gate 未放行 ⇒ subscribing 必在），B 落入等待
	// 分支阻塞在 call.done 上。
	secondErr := make(chan error, 1)
	go func() {
		secondErr <- c.SubscribeService(ctx, "svc", func([]registry.Instance) {
			atomic.AddInt32(&bHits, 1)
		})
	}()
	// 等 B 落位：轮询到 B 的条目已追加（len==2），再给一段 settle 时间
	// 让 B 走完「加锁检查 → 解锁 → select 停车」的无竞争路径（与仓内 #95
	// 回归测试 TestSubscribe_CtxDeadlineDuringPending_NoCallbackLeak 的
	// 停车启发式同款）。gate 放行只发生在 B 落位之后；固定代码下无论 B
	// 停在等待分支还是直接走去重路径，条目都应幸存 ⇒ 本测试在修复后
	// 无竞态失败面。
	c.callbackMu.Lock()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(c.instanceCallbacks["svc"]) < 2 {
		c.callbackMu.Unlock()
		time.Sleep(2 * time.Millisecond)
		c.callbackMu.Lock()
	}
	svcLen := len(c.instanceCallbacks["svc"])
	c.callbackMu.Unlock()
	if svcLen < 2 {
		t.Fatalf("B 的回调条目未落位（len=%d）", svcLen)
	}
	time.Sleep(100 * time.Millisecond) // settle：B 进入 select 停车

	// 放行 A 的注册；A 成功 ⇒ B 经 call.done 醒来走去重路径返回 nil
	close(sub.gate)
	if err := <-firstErr; err != nil {
		t.Fatalf("A subscribe: %v", err)
	}
	if err := <-secondErr; err != nil {
		t.Fatalf("B subscribe（等待成功醒来应走去重路径返回 nil）: %v", err)
	}

	// 模拟 provider 推送实例变更：扇出必须同时到达 A 与 B
	sub.mu.Lock()
	dispatch := sub.lastCB
	sub.mu.Unlock()
	if dispatch == nil {
		t.Fatal("provider 派发回调未被记录")
	}
	dispatch([]registry.Instance{{Service: "svc", IP: "10.0.0.1", Port: 8080}})

	pushDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(pushDeadline) && atomic.LoadInt32(&aHits)+atomic.LoadInt32(&bHits) < 2 {
		time.Sleep(2 * time.Millisecond)
	}
	if atomic.LoadInt32(&aHits) != 1 {
		t.Fatalf("A hits = %d, want 1", aHits)
	}
	if atomic.LoadInt32(&bHits) != 1 {
		t.Fatalf("B hits = %d, want 1（#104 回归：B 订阅成功但回调条目在等待期被提前摘除，推送扇出不可达）", bHits)
	}

	// 二次推送同样可达（去重路径的一次性初始回调不算数，变更推送持续可达）
	dispatch([]registry.Instance{{Service: "svc", IP: "10.0.0.2", Port: 8080}})
	pushDeadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(pushDeadline) && atomic.LoadInt32(&aHits)+atomic.LoadInt32(&bHits) < 4 {
		time.Sleep(2 * time.Millisecond)
	}
	if atomic.LoadInt32(&aHits) != 2 || atomic.LoadInt32(&bHits) != 2 {
		t.Fatalf("after 2nd push: A=%d B=%d, want 2/2", aHits, bHits)
	}
}
