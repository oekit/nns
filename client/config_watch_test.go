package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// TestOnConfigChange_LateRegistrationReceivesEvents 回归测试（W1-T3）：
// 回调列表必须在事件分发时现读，WatchService 之后注册的回调同样要收到事件。
// 修复前 WatchService 注册时快照回调列表，晚注册的回调永远收不到事件。
func TestOnConfigChange_LateRegistrationReceivesEvents(t *testing.T) {
	store := registrymocks.NewConfigStore()
	c := newTestClient(t, WithConfigStore(store))

	c.WatchService(context.Background(), "svc-a")
	waitListening(t, store, "svc-a", "DEFAULT_GROUP")

	var fired atomic.Bool
	c.OnConfigChange(func(serviceName string) {
		if serviceName == "svc-a" {
			fired.Store(true)
		}
	})

	store.FireChange("svc-a", "DEFAULT_GROUP", "")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !fired.Load() {
		time.Sleep(2 * time.Millisecond)
	}
	if !fired.Load() {
		t.Fatal("callback registered after WatchService did not receive the config change event")
	}
}

// TestOnConfigChange_PanicIsolated 回归测试（W1-M11）：
// 用户回调 panic 不得击穿 provider SDK 的回调 goroutine，
// 且不能影响后续回调的执行。
func TestOnConfigChange_PanicIsolated(t *testing.T) {
	store := registrymocks.NewConfigStore()
	c := newTestClient(t, WithConfigStore(store))

	c.WatchService(context.Background(), "svc-a")
	waitListening(t, store, "svc-a", "DEFAULT_GROUP")

	var second atomic.Bool
	c.OnConfigChange(func(string) { panic("user callback exploded") })
	c.OnConfigChange(func(string) { second.Store(true) })

	store.FireChange("svc-a", "DEFAULT_GROUP", "")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !second.Load() {
		time.Sleep(2 * time.Millisecond)
	}
	if !second.Load() {
		t.Fatal("callback after a panicking callback was not invoked")
	}
}

// TestWatchService_ListenFailureAllowsRetry 回归测试（W1-M12）：
// ListenConfig 失败必须回滚去重标记，允许下次 WatchService 重试。
// 修复前一次瞬态失败会永久禁用该服务的配置热更新。
func TestWatchService_ListenFailureAllowsRetry(t *testing.T) {
	store := registrymocks.NewConfigStore()
	store.SetErr(errors.New("nacos down"))

	c := newTestClient(t, WithConfigStore(store))
	c.WatchService(context.Background(), "svc-a")

	// 等待失败路径完成回滚
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.RLock()
		watched := c.watchedServices["svc-a"]
		c.mu.RUnlock()
		if !watched {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// nacos 恢复后重试，监听必须能注册成功
	store.SetErr(nil)

	c.WatchService(context.Background(), "svc-a")
	waitListening(t, store, "svc-a", "DEFAULT_GROUP")
}

// TestClose_ClearsWatchState 回归测试（W1-T3）：
// Close 必须清空监听与回调状态。Close 是终结操作（默认路径的 SDK 客户端
// 已被关闭，gRPC 连接不可复用），Close 后 WatchService 不再注册、
// 状态保持清空——复用需重新 New 一个实例。
func TestClose_ClearsWatchState(t *testing.T) {
	store := registrymocks.NewConfigStore()
	c := newTestClient(t, WithConfigStore(store))

	c.WatchService(context.Background(), "svc-a")
	waitListening(t, store, "svc-a", "DEFAULT_GROUP")

	c.Close()

	c.mu.RLock()
	watched := len(c.watchedServices)
	callbacks := len(c.configChangeCallbacks)
	c.mu.RUnlock()
	if watched != 0 || callbacks != 0 {
		t.Fatalf("expected watch state cleared, got watched=%d callbacks=%d", watched, callbacks)
	}

	// Close 后（终结语义）：重新 WatchService 是 no-op，不注册、不残留
	c.WatchService(context.Background(), "svc-a")

	c.mu.RLock()
	watchedAfter := len(c.watchedServices)
	c.mu.RUnlock()
	if watchedAfter != 0 {
		t.Fatalf("WatchService after Close must be a no-op, watched=%d", watchedAfter)
	}
}

// TestSubscribeService_DedupSDKListener 回归测试（W1-M10）：
// 同一服务多次订阅只向 provider 注册一次 SDK 监听，
// 推送时扇出到全部本地回调（修复前每次订阅都注册新监听 → N² 分发）。
func TestSubscribeService_DedupSDKListener(t *testing.T) {
	sub := registrymocks.NewSubscriber()
	c := newTestClient(t, WithSubscriber(sub))

	for i := 0; i < 3; i++ {
		if err := c.SubscribeService(context.Background(), "svc-a", func([]registry.Instance) {}); err != nil {
			t.Fatalf("Subscribe #%d: %v", i, err)
		}
	}

	if n := sub.CallbackCount("svc-a"); n != 1 {
		t.Fatalf("expected exactly 1 SDK listener for svc-a, got %d", n)
	}

	// 一次推送必须触发全部 3 个本地回调
	var count atomic.Int32
	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		if err := c.SubscribeService(context.Background(), "svc-b", func([]registry.Instance) {
			defer wg.Done()
			count.Add(1)
		}); err != nil {
			t.Fatalf("Subscribe svc-b #%d: %v", i, err)
		}
	}
	// svc-b 此时已注册 1 个 SDK 监听（第一个 Subscribe 时），直接触发即可
	sub.Push("svc-b", nil)
	wg.Wait()
	if got := count.Load(); got != 3 {
		t.Fatalf("expected 3 callback invocations from a single push, got %d", got)
	}
}

// TestSubscribeService_CallbackPanicIsolated 回归测试（W1-M11）：
// 实例推送回调 panic 不得击穿分发循环，后续回调正常执行。
func TestSubscribeService_CallbackPanicIsolated(t *testing.T) {
	sub := registrymocks.NewSubscriber()
	c := newTestClient(t, WithSubscriber(sub))

	var second atomic.Bool
	_ = c.SubscribeService(context.Background(), "svc-a", func([]registry.Instance) { panic("boom") })
	_ = c.SubscribeService(context.Background(), "svc-a", func([]registry.Instance) { second.Store(true) })

	// 第二次订阅复用同一 SDK 监听（去重），触发一次即可
	if n := sub.CallbackCount("svc-a"); n != 1 {
		t.Fatalf("expected 1 SDK listener, got %d", n)
	}
	sub.Push("svc-a", nil)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !second.Load() {
		time.Sleep(2 * time.Millisecond)
	}
	if !second.Load() {
		t.Fatal("callback after a panicking callback was not invoked")
	}
}

// TestOffConfigChange_RemovesCallback 回归测试（nns-endpoint#135 对侧）：
// OffConfigChange 按注册句柄精确摘除单个回调，其余回调不受影响；
// 摘除后配置事件不再送达被注销的回调。
func TestOffConfigChange_RemovesCallback(t *testing.T) {
	store := registrymocks.NewConfigStore()
	c := newTestClient(t, WithConfigStore(store))

	c.WatchService(context.Background(), "svc-a")
	waitListening(t, store, "svc-a", "DEFAULT_GROUP")

	var kept, removed atomic.Int32
	h1 := c.OnConfigChange(func(string) { kept.Add(1) })
	h2 := c.OnConfigChange(func(string) { removed.Add(1) })

	c.OffConfigChange(h2)
	store.FireChange("svc-a", "DEFAULT_GROUP", "")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && kept.Load() == 0 {
		time.Sleep(2 * time.Millisecond)
	}
	if kept.Load() == 0 {
		t.Fatal("surviving callback did not receive the config change event")
	}
	// 给可能误触发的分发留出窗口，再断言被注销回调零送达
	time.Sleep(50 * time.Millisecond)
	if got := removed.Load(); got != 0 {
		t.Fatalf("removed callback received %d events after OffConfigChange", got)
	}
	_ = h1
}

// TestOffConfigChange_Idempotent 钉住注销幂等边界：nil 句柄、重复注销、
// 摘除后句柄再注销均为 no-op，不 panic、不影响其余回调。
func TestOffConfigChange_Idempotent(t *testing.T) {
	store := registrymocks.NewConfigStore()
	c := newTestClient(t, WithConfigStore(store))

	c.OffConfigChange(nil) // nil 句柄 no-op

	handle := c.OnConfigChange(func(string) {})

	c.OffConfigChange(handle)
	c.OffConfigChange(handle) // 重复注销 no-op

	c.mu.RLock()
	count := len(c.configChangeCallbacks)
	c.mu.RUnlock()
	if count != 0 {
		t.Fatalf("expected callback list empty after unregister, got %d", count)
	}
}

// TestOnConfigChange_ReturnsDistinctHandles 钉住句柄唯一性：同一回调函数
// 注册两次得到两个独立句柄，注销其一不影响另一注册（按句柄而非函数身份
// 摘除——Go 函数值不可比较，同一闭包字面量的两次构造无法按函数区分）。
func TestOnConfigChange_ReturnsDistinctHandles(t *testing.T) {
	c := newTestClient(t)

	var total atomic.Int32
	fn := func(string) { total.Add(1) }
	h1 := c.OnConfigChange(fn)
	h2 := c.OnConfigChange(fn)
	if h1 == h2 {
		t.Fatal("expected distinct handles for two registrations")
	}

	c.OffConfigChange(h1)
	c.mu.RLock()
	count := len(c.configChangeCallbacks)
	c.mu.RUnlock()
	if count != 1 {
		t.Fatalf("expected 1 callback after unregistering one of two, got %d", count)
	}
}
