package client

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/oekit/registry"
)

// tornDownSubscriber 模拟「在途退订会吞掉并发新监听」的 provider 语义
// （#147 的前提：Nacos 类 SDK 的 Unsubscribe 与 Subscribe 在 provider 侧
// 交错时，后落地的 Unsubscribe 可把窗口内刚建立的新监听一并拆除——
// 本 fake 把这条真实交错钉成可断言行为：Unsubscribe 在途期间到达的
// Subscribe 返回成功但**不建立**监听）。
type tornDownSubscriber struct {
	mu            sync.Mutex
	active        int  // provider 侧存活监听数
	unsubInFlight bool // 有 Unsubscribe 在途
	subCalls      int

	unsubEntered chan struct{} // 首次进入 provider.Unsubscribe 时关闭
	releaseUnsub chan struct{} // 放行在途 Unsubscribe
	enteredOnce  sync.Once

	lastDispatch func([]registry.Instance)
}

func newTornDownSubscriber() *tornDownSubscriber {
	return &tornDownSubscriber{
		unsubEntered: make(chan struct{}),
		releaseUnsub: make(chan struct{}),
	}
}

func (s *tornDownSubscriber) Subscribe(_ context.Context, _ string, cb func([]registry.Instance)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subCalls++
	if s.unsubInFlight {
		// 看似成功，实际监听被在途退订吞掉——这正是旧实现踩中的窗口
		return nil
	}
	s.active++
	s.lastDispatch = cb
	return nil
}

func (s *tornDownSubscriber) Unsubscribe(_ context.Context, _ string) error {
	s.mu.Lock()
	s.unsubInFlight = true
	if s.active > 0 {
		s.active--
	}
	s.mu.Unlock()
	s.enteredOnce.Do(func() { close(s.unsubEntered) })
	<-s.releaseUnsub
	s.mu.Lock()
	s.unsubInFlight = false
	s.mu.Unlock()
	return nil
}

func (s *tornDownSubscriber) snapshot() (active, subCalls int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active, s.subCalls
}

// TestUnsubscribeService_ConcurrentResubscribe_NotTornDown 回归 #147：
// UnsubscribeService 清完本地状态、provider 退订**在途**的窗口里，并发的
// SubscribeService 不得「看似订阅成功、监听随即被在途退订拆除」——那之后
// 本地 subscribedServices 已置位 ⇒ 永不重注册 ⇒ 实例变更推送全部丢失，
// 服务路由静默僵死。
//
// 旧实现红：新订阅不等在途退订，Subscribe 落在退订窗口内被吞（provider
// 存活监听 0 个），本地却标记已订阅。
// 新实现绿：ensureSubscribe 见 unsubscribing 标记等待退订落地后接手注册，
// provider 存活监听恢复 1 个，推送可达。
func TestUnsubscribeService_ConcurrentResubscribe_NotTornDown(t *testing.T) {
	sub := newTornDownSubscriber()
	c := newTestClient(t, WithSubscriber(sub))
	ctx := context.Background()

	// 1. 首次订阅：监听建立（active=1）。
	if err := c.SubscribeService(ctx, "svc", func([]registry.Instance) {}); err != nil {
		t.Fatalf("首次订阅失败: %v", err)
	}
	if active, _ := sub.snapshot(); active != 1 {
		t.Fatalf("首次订阅后 provider 存活监听 = %d, want 1", active)
	}

	// 2. 后台退订：进入 provider.Unsubscribe 后挂起——此刻本地状态已清
	//    （新旧实现的「清状态 → 调 provider」顺序一致），窗口开启。
	unsubErr := make(chan error, 1)
	go func() { unsubErr <- c.UnsubscribeService(ctx, "svc") }()
	select {
	case <-sub.unsubEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("退订未进入 provider 阶段")
	}

	// 3. 窗口内并发新订阅：新实现必须等在途退订落地后再注册。
	resubErr := make(chan error, 1)
	go func() {
		resubErr <- c.SubscribeService(ctx, "svc", func([]registry.Instance) {})
	}()

	// 确定性落位：等新订阅的回调条目已追加（其 Subscribe 调用链此后立即
	// 决策），再给它一小段收场时间——
	//   旧实现：不等退订、立即「被吞注册」并返回（resubErr 很快有值）；
	//   新实现：停在 unsubscribing 等待分支（resubErr 不会提前有值）。
	// 以「resubErr 到达或超时」为放行条件，两种实现下订阅决策都必然
	// 已发生在退订窗口内。
	c.callbackMu.Lock()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(c.instanceCallbacks["svc"]) < 1 {
		c.callbackMu.Unlock()
		time.Sleep(2 * time.Millisecond)
		c.callbackMu.Lock()
	}
	c.callbackMu.Unlock()
	if len(c.instanceCallbacks["svc"]) < 1 {
		t.Fatal("并发新订阅的回调条目未落位")
	}
	var resubResult error
	resubReceived := false
	select {
	case resubResult = <-resubErr:
		resubReceived = true
	case <-time.After(300 * time.Millisecond):
		// 新实现：仍停在等待在途退订的分支——正是被修复的语义。
	}

	// 4. 放行退订并等两侧收场。
	close(sub.releaseUnsub)
	if err := <-unsubErr; err != nil {
		t.Fatalf("退订失败: %v", err)
	}
	if !resubReceived {
		select {
		case resubResult = <-resubErr:
		case <-time.After(3 * time.Second):
			t.Fatal("并发新订阅未返回（ensureSubscribe 疑似死等在途退订）")
		}
	}
	if resubResult != nil {
		t.Fatalf("并发新订阅失败: %v", resubResult)
	}

	// 5. 核心断言：provider 侧存活监听必须为 1。旧实现在窗口内被吞，
	//    存活 0 个而本地标记已订阅（僵尸态）；新实现退订落地后重注册。
	active, _ := sub.snapshot()
	if active != 1 {
		t.Fatalf("#147 回归：provider 存活监听 = %d, want 1（在途退订吞掉了并发新订阅，本地却标记已订阅 ⇒ 推送永久丢失）", active)
	}
	c.callbackMu.RLock()
	subscribed := c.subscribedServices["svc"]
	callbacks := len(c.instanceCallbacks["svc"])
	c.callbackMu.RUnlock()
	if !subscribed {
		t.Fatal("并发新订阅后本地 subscribedServices 标记应在位")
	}
	if callbacks != 1 {
		t.Fatalf("扇出回调数 = %d, want 1", callbacks)
	}

	// 6. 端到端：provider 推送必须可达新回调（监听真实存活，非纸面标记）。
	pushed := make(chan int, 1)
	if err := c.SubscribeService(ctx, "svc", func(is []registry.Instance) {
		pushed <- len(is)
	}); err != nil {
		t.Fatalf("补一次可观测回调订阅失败: %v", err)
	}
	sub.mu.Lock()
	dispatch := sub.lastDispatch
	sub.mu.Unlock()
	if dispatch == nil {
		t.Fatal("订阅未向 provider 注册派发回调")
	}
	dispatch([]registry.Instance{{Service: "svc", IP: "10.0.0.9", Port: 8080}})
	select {
	case n := <-pushed:
		if n != 1 {
			t.Fatalf("推送实例数 = %d, want 1", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("#147 回归：provider 推送未达新订阅回调（监听未真实存活）")
	}
}

// TestUnsubscribeService_ConcurrentUnsubscribe_BothReturn 并发双退订：后来者
// 等待在途退订结束后重入（provider 侧幂等），两调用都必须返回、无死锁。
func TestUnsubscribeService_ConcurrentUnsubscribe_BothReturn(t *testing.T) {
	sub := newTornDownSubscriber()
	c := newTestClient(t, WithSubscriber(sub))
	ctx := context.Background()

	if err := c.SubscribeService(ctx, "svc", func([]registry.Instance) {}); err != nil {
		t.Fatalf("首次订阅失败: %v", err)
	}

	// 首个退订进入 provider 阶段后挂起，第二个退订并发进入。
	firstErr := make(chan error, 1)
	go func() { firstErr <- c.UnsubscribeService(ctx, "svc") }()
	select {
	case <-sub.unsubEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("首个退订未进入 provider 阶段")
	}
	secondErr := make(chan error, 1)
	go func() { secondErr <- c.UnsubscribeService(ctx, "svc") }()

	close(sub.releaseUnsub)
	for i, ch := range []chan error{firstErr, secondErr} {
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatalf("并发退订 %d 未返回（死锁）", i)
		}
	}
	if active, _ := sub.snapshot(); active != 0 {
		t.Fatalf("全部退订后 provider 存活监听 = %d, want 0", active)
	}
}
