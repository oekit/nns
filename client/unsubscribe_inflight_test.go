package client

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// TestUnsubscribeService_WaitsWholeInflightSubscribe 回归 #173：UnsubscribeService
// 必须等**全部在途 SubscribeService 出结果**（在途槽的 done 关闭）才能落地
// provider 退订。在途槽与回调条目同帧发布、覆盖订阅调用全程（条目登记、
// 等待、去重初始回调、provider 注册）。
//
// 旧实现红：槽在 ensureSubscribe 成为注册方时才登记——「条目已落表、槽未
// 登记」的登记窗口与去重初始回调期对退订不可见，退订可整段插队：provider
// 退订先落地、在途订阅随后注册监听 ⇒ 已退订服务被幽灵复活，退订谎报成功。
// 本用例用去重初始回调（用户回调挂起）钉住该窗口：新实现下退订必须等回调
// 收场才返回；旧实现退订在回调仍挂起时已返回。
func TestUnsubscribeService_WaitsWholeInflightSubscribe(t *testing.T) {
	sub := registrymocks.NewSubscriber()
	c := newTestClient(t,
		WithSubscriber(sub),
		// 静态实例：保证去重路径的初始回调必然触发（回调挂起可观测）
		WithStaticInstances("svc", []registry.Instance{{Service: "svc", IP: "10.0.0.1", Port: 8080}}),
	)
	ctx := context.Background()

	// 1. 首次订阅完整收场：监听存活、subscribedServices 置位。
	if err := c.SubscribeService(ctx, "svc", func([]registry.Instance) {}); err != nil {
		t.Fatalf("首次订阅失败: %v", err)
	}

	// 2. 并发第二次订阅：走去重路径，其初始回调（用户回调）挂起——
	//    该订阅处于「在途」状态（新实现：在途槽在位）。
	cbEntered := make(chan struct{})
	cbRelease := make(chan struct{})
	var cbOnce sync.Once
	s2Done := make(chan error, 1)
	go func() {
		s2Done <- c.SubscribeService(ctx, "svc", func([]registry.Instance) {
			cbOnce.Do(func() { close(cbEntered) })
			<-cbRelease
		})
	}()
	select {
	case <-cbEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("第二次订阅的去重初始回调未落位")
	}

	// 3. 退订并发进入：必须等在途订阅整体收场（含初始回调）。
	unsubDone := make(chan error, 1)
	go func() { unsubDone <- c.UnsubscribeService(ctx, "svc") }()

	// 核心断言：退订不得先于在途订阅收场返回。
	// 旧实现：去重初始回调期无在途标记，退订立即返回（此处转红）。
	// 新实现：退订停在 subscribing 槽等待分支。
	select {
	case err := <-unsubDone:
		t.Fatalf("#173 回归：UnsubscribeService 在在途订阅收场前返回（err=%v）——登记/去重初始回调期对退订不可见，provider 退订可插队复活在途订阅", err)
	case <-time.After(300 * time.Millisecond):
		// 新实现预期：仍在等待在途订阅收场。
	}

	// 4. 放行回调 → 在途订阅收场 → 退订随后落地。
	close(cbRelease)
	if err := <-s2Done; err != nil {
		t.Fatalf("第二次订阅失败: %v", err)
	}
	select {
	case err := <-unsubDone:
		if err != nil {
			t.Fatalf("UnsubscribeService: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("UnsubscribeService 未返回（疑似死等在途订阅）")
	}

	// 5. 终态断言：退订后无登记、无标记、provider 监听已注销。
	c.callbackMu.RLock()
	callbacks := len(c.instanceCallbacks["svc"])
	subscribed := c.subscribedServices["svc"]
	c.callbackMu.RUnlock()
	if callbacks != 0 {
		t.Fatalf("#173 回归：退订后扇出登记 = %d, want 0", callbacks)
	}
	if subscribed {
		t.Fatal("#173 回归：退订后 subscribedServices 应已摘除")
	}
	if n := sub.CallbackCount("svc"); n != 0 {
		t.Fatalf("#173 回归：退订后 provider 侧监听 = %d, want 0（监听被在途订阅复活）", n)
	}
}

// TestUnsubscribeService_InflightSubscribeNotResurrected_Stress 是 #173 的
// -race 压力回归钉：Subscribe 先行（fence：回调条目已落表）、Unsubscribe
// 随后并发进入，N 次交错。终态断言「退订后无登记且监听不复活」——
// 新实现在途槽与条目同帧发布，退订必然等在途订阅整体收场，provider 侧
// 次序确定为「订阅落地 → 退订落地」，断言确定性成立；旧实现退订可插进
// 「条目已落表、槽未登记」的窗口（随后订阅复活监听），交错命中即红。
func TestUnsubscribeService_InflightSubscribeNotResurrected_Stress(t *testing.T) {
	for i := 0; i < 200; i++ {
		sub := registrymocks.NewSubscriber()
		c := newTestClient(t, WithSubscriber(sub))
		ctx := context.Background()

		subDone := make(chan error, 1)
		go func() {
			subDone <- c.SubscribeService(ctx, "svc", func([]registry.Instance) {})
		}()

		// fence：等订阅的回调条目落表再发起退订（新旧实现下「条目落表」
		// 都可观测；新实现下它同时意味着在途槽已在位）。
		c.callbackMu.Lock()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && len(c.instanceCallbacks["svc"]) < 1 {
			c.callbackMu.Unlock()
			time.Sleep(time.Millisecond)
			c.callbackMu.Lock()
		}
		c.callbackMu.Unlock()
		if len(c.instanceCallbacks["svc"]) < 1 {
			t.Fatalf("iteration %d: 订阅条目未落位", i)
		}

		unsubDone := make(chan error, 1)
		go func() { unsubDone <- c.UnsubscribeService(ctx, "svc") }()

		if err := <-subDone; err != nil {
			t.Fatalf("iteration %d: subscribe: %v", i, err)
		}
		if err := <-unsubDone; err != nil {
			t.Fatalf("iteration %d: unsubscribe: %v", i, err)
		}

		// 终态：退订后无登记、无标记、provider 监听不复活。
		c.callbackMu.RLock()
		callbacks := len(c.instanceCallbacks["svc"])
		subscribed := c.subscribedServices["svc"]
		c.callbackMu.RUnlock()
		if callbacks != 0 || subscribed {
			t.Fatalf("#173 回归（iteration %d）：退订与在途订阅交错后终态不干净 callbacks=%d subscribed=%v——在途订阅在退订落地后复活了监听", i, callbacks, subscribed)
		}
		if n := sub.CallbackCount("svc"); n != 0 {
			t.Fatalf("#173 回归（iteration %d）：provider 监听 = %d, want 0", i, n)
		}
	}
}
