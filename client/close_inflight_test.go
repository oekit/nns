package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/oekit/registry"
)

// inflightRecorder 记录 Subscribe/Unsubscribe 的调用顺序（#152）：Subscribe
// 阻塞到 gate 放行，供测试精确控制「provider 侧注册落地」与「Close 注销」
// 的相对时序。
type inflightRecorder struct {
	mu     sync.Mutex
	events []string
	gate   chan struct{}
}

func newInflightRecorder() *inflightRecorder {
	return &inflightRecorder{gate: make(chan struct{})}
}

func (s *inflightRecorder) record(ev string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
}

func (s *inflightRecorder) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

func (s *inflightRecorder) Subscribe(ctx context.Context, service string, _ func([]registry.Instance)) error {
	s.record("subscribe:" + service)
	// 尊重 ctx：并发生命周期压测（ConcurrentCloseSubscribe_NoRace）的注册方
	// 携带短超时 ctx，gate 不放行时靠 ctx.Done 收场——忽略 ctx 会永久挂住
	// 注册方（真实 SDK 都响应取消）。
	select {
	case <-s.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (s *inflightRecorder) Unsubscribe(_ context.Context, service string) error {
	s.record("unsubscribe:" + service)
	return nil
}

// countEvents 统计带前缀的事件数。
func countEvents(events []string, prefix string) int {
	n := 0
	for _, ev := range events {
		if len(ev) >= len(prefix) && ev[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

// TestCloseWithContext_WaitsInflightSubscribe_CompensatesUnsubscribe 回归 #152：
// Close 不等在途 Subscribe 完成 ⇒ SDK 监听可落在 Unsubscribe 之后永久残留，
// 且已关闭的 client 被复写回 subscribedServices。
//
// 修法两层：
//  1. CloseWithContext 先等在途注册出结果（closeCtx 预算内）再注销；
//  2. ensureSubscribe 成功路径复查 closed：命中则不复写 subscribedServices，
//     并立即补偿注销（对齐 WatchService 注册成功后的 closed 复查补偿）。
//
// 本用例钉第 2 层的补偿语义：Subscribe 先于 Close 通过入口检查、注册期间
// Close 完成 —— 注册「成功」返回但监听已被补偿注销，且 subscribedServices
// 保持清空。变异性负对照：去掉 publish 时的 closed 复查补偿后，本用例
// 在「unsubscribe 事件数 == 0」断言处转红（监听残留）。
func TestCloseWithContext_WaitsInflightSubscribe_CompensatesUnsubscribe(t *testing.T) {
	sub := newInflightRecorder()
	c := newTestClient(t, WithSubscriber(sub))

	ctx := context.Background()
	// 订阅方进入 provider 注册（阻塞在 gate 上，subscribing 在途）
	subErr := make(chan error, 1)
	go func() { subErr <- c.SubscribeService(ctx, "svc-closed", func([]registry.Instance) {}) }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && countEvents(sub.snapshot(), "subscribe:") == 0 {
		time.Sleep(2 * time.Millisecond)
	}

	// Close 落地：closed 置位、本地状态清空；等待在途注册出结果（挂住）
	closeErr := make(chan error, 1)
	go func() { closeErr <- c.CloseWithContext(context.Background()) }()
	time.Sleep(100 * time.Millisecond) // settle：Close 走到等待点

	// 等待期间不得有 Unsubscribe：在途注册还没出结果，先注销就是 #152 的
	// 「注册落在注销之后」窗口
	if got := countEvents(sub.snapshot(), "unsubscribe:"); got != 0 {
		t.Fatalf("Close issued Unsubscribe before in-flight Subscribe resolved (%d events): %v",
			got, sub.snapshot())
	}

	// 放行注册：Subscribe 成功返回 ⇒ publish 时 closed 复查命中 ⇒ 补偿注销
	close(sub.gate)
	if err := <-subErr; err != nil {
		t.Fatalf("in-flight subscribe should return nil (registration succeeded, then compensated): %v", err)
	}
	select {
	case err := <-closeErr:
		if err != nil {
			t.Fatalf("CloseWithContext: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CloseWithContext did not return after in-flight subscribe resolved")
	}

	events := sub.snapshot()
	if got := countEvents(events, "unsubscribe:svc-closed"); got != 1 {
		t.Fatalf("#152 regression: expected exactly 1 compensating unsubscribe, got %d (events=%v)", got, events)
	}
	// 顺序：补偿注销必须发生在 provider 侧注册之后（先注册后注销，无残留窗口）
	if len(events) < 2 || events[0] != "subscribe:svc-closed" || events[1] != "unsubscribe:svc-closed" {
		t.Fatalf("event order must be subscribe -> unsubscribe, got %v", events)
	}
	// 已关闭的 client 不得被复写回「已订阅」
	c.callbackMu.Lock()
	marked := c.subscribedServices["svc-closed"]
	c.callbackMu.Unlock()
	if marked {
		t.Fatal("#152 regression: closed client was re-marked as subscribed")
	}
}

// TestSubscribeService_WaiterAfterClose_ReturnsErrClientClosed 回归 #152 的
// 等待者面：等待在途注册的订阅方在 Close 之后醒来，不得接手向已关闭的
// provider 发起新注册（旧代码会重新注册 ⇒ 监听残留），必须以
// ErrClientClosed 收场并回滚自身条目。
func TestSubscribeService_WaiterAfterClose_ReturnsErrClientClosed(t *testing.T) {
	sub := newInflightRecorder()
	c := newTestClient(t, WithSubscriber(sub))

	ctx := context.Background()
	// A 成为在途注册方（阻塞在 gate）
	aErr := make(chan error, 1)
	go func() { aErr <- c.SubscribeService(ctx, "svc-wait", func([]registry.Instance) {}) }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && countEvents(sub.snapshot(), "subscribe:") == 0 {
		time.Sleep(2 * time.Millisecond)
	}

	// B 落入等待分支（A 在途）
	bErr := make(chan error, 1)
	go func() { bErr <- c.SubscribeService(ctx, "svc-wait", func([]registry.Instance) {}) }()
	time.Sleep(100 * time.Millisecond) // settle：B 进入 select 停车

	// Close 落地（会等 A 出结果，此处只置位 closed 并清空状态）
	closeErr := make(chan error, 1)
	go func() { closeErr <- c.CloseWithContext(context.Background()) }()
	time.Sleep(100 * time.Millisecond)

	// 放行 A：A「注册成功」被补偿注销；B 醒来后必须走 closed 复查返回
	// ErrClientClosed，而不是接手注册
	close(sub.gate)
	if err := <-aErr; err != nil {
		t.Fatalf("A (registrant, compensated): %v", err)
	}
	select {
	case err := <-bErr:
		if !errors.Is(err, ErrClientClosed) {
			t.Fatalf("#152 regression: waiter after close must return ErrClientClosed, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("B (waiter) did not return after close")
	}
	if err := <-closeErr; err != nil {
		t.Fatalf("CloseWithContext: %v", err)
	}
	// B 的条目已回滚、A 的监听已补偿注销：provider 只收到一次注册 + 一次注销
	events := sub.snapshot()
	if got := countEvents(events, "subscribe:svc-wait"); got != 1 {
		t.Fatalf("provider must receive exactly 1 Subscribe call, got %d (events=%v)", got, events)
	}
	if got := countEvents(events, "unsubscribe:svc-wait"); got != 1 {
		t.Fatalf("provider must receive exactly 1 Unsubscribe call, got %d (events=%v)", got, events)
	}
}

// TestEnsureSubscribe_ClosedAtLoopTop_NoProviderCall 单元钉 #152 的循环顶
// closed 复查：SubscribeService 入口检查挡住「关闭后才发起」的调用，但
// ensureSubscribe 的等待循环（等在途注册/退订）每轮醒来都会重新决策——
// 决策前必须复查 closed，否则「发起时未关闭、等待期间 Close 完成」的调用
// 会向已关闭的 SDK 发起新注册。
func TestEnsureSubscribe_ClosedAtLoopTop_NoProviderCall(t *testing.T) {
	sub := newInflightRecorder()
	c := newTestClient(t, WithSubscriber(sub))

	// 模拟「等待期间 Close 完成」：closed 置位 + 一个已出结果的在途退订，
	// 让循环顶先撞上 closed 复查（早于 unsubscribing 等待的重入）
	c.closed.Store(true)
	done := make(chan struct{})
	close(done)
	c.callbackMu.Lock()
	if c.unsubscribing == nil {
		c.unsubscribing = make(map[string]*subscribeCall)
	}
	c.unsubscribing["svc-looptop"] = &subscribeCall{done: done}
	c.callbackMu.Unlock()

	entry := &instanceCallback{fn: func([]registry.Instance) {}}
	_, err := c.ensureSubscribe(context.Background(), "svc-looptop", entry)
	if !errors.Is(err, ErrClientClosed) {
		t.Fatalf("ensureSubscribe after close must return ErrClientClosed, got %v", err)
	}
	if got := countEvents(sub.snapshot(), "subscribe:"); got != 0 {
		t.Fatalf("closed client must not issue provider Subscribe, got %d", got)
	}
}

// TestCloseWithContext_ConcurrentCloseSubscribe_NoRace 是 #96 用例的 #152
// 加强版：Close 与 SubscribeService 并发（含在途注册被 Close 等待/补偿的
// 全部交错），-race 下作为生命周期竞争的回归钉。
func TestCloseWithContext_ConcurrentCloseSubscribe_NoRace(t *testing.T) {
	sub := newInflightRecorder()
	c := newTestClient(t, WithSubscriber(sub))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_ = c.SubscribeService(ctx, "svc-lifecycle", func([]registry.Instance) {})
		}()
	}
	wg.Add(1)
	go func() { defer wg.Done(); _ = c.CloseWithContext(context.Background()) }()
	wg.Wait()
}
