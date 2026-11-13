package client

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// gatedConfigStore 可将 ListenConfig / StopListenConfig 挂起在 gate 上
// （#170 行为面测试用）：卡住 ListenConfig 等价于真实 SDK 的「注册在途」
// 窗口——去重标记已置位、provider 侧还没有监听。
type gatedConfigStore struct {
	*registrymocks.ConfigStore

	listenEntered chan struct{}
	listenRelease chan struct{}
	listenOnce    sync.Once

	stopEntered chan struct{}
	stopRelease chan struct{}
	stopOnce    sync.Once
}

func newGatedConfigStore() *gatedConfigStore {
	return &gatedConfigStore{
		ConfigStore:   registrymocks.NewConfigStore(),
		listenEntered: make(chan struct{}),
		listenRelease: make(chan struct{}),
		stopEntered:   make(chan struct{}),
		stopRelease:   make(chan struct{}),
	}
}

func (g *gatedConfigStore) ListenConfig(ctx context.Context, dataID, group string, onChange func(string)) error {
	g.listenOnce.Do(func() { close(g.listenEntered) })
	select {
	case <-g.listenRelease:
	case <-ctx.Done():
		return ctx.Err()
	}
	return g.ConfigStore.ListenConfig(ctx, dataID, group, onChange)
}

func (g *gatedConfigStore) StopListenConfig(ctx context.Context, dataID, group string) error {
	g.stopOnce.Do(func() { close(g.stopEntered) })
	select {
	case <-g.stopRelease:
	case <-ctx.Done():
		return ctx.Err()
	}
	return g.ConfigStore.StopListenConfig(ctx, dataID, group)
}

// 编译期守卫：确认 gatedConfigStore 实现 registry.ConfigStore。
var _ registry.ConfigStore = (*gatedConfigStore)(nil)

// stopFailConfigStore 在 gatedConfigStore 之上注入 StopListenConfig 失败
// （短路返回，不触达被包装实例的 Stop 闸）。
type stopFailConfigStore struct {
	registry.ConfigStore
	stopErr error
}

func (f *stopFailConfigStore) StopListenConfig(ctx context.Context, dataID, group string) error {
	if f.stopErr != nil {
		return f.stopErr
	}
	return f.ConfigStore.StopListenConfig(ctx, dataID, group)
}

// 编译期守卫：确认 stopFailConfigStore 实现 registry.ConfigStore。
var _ registry.ConfigStore = (*stopFailConfigStore)(nil)

// waitListeningCount 等待指定 dataID/group 的监听数达到 want（gated
// ConfigStore 形态的 waitListening）。
func waitListeningCount(t *testing.T, s registry.ConfigStore, dataID, group string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.(*gatedConfigStore).ListeningCountFor(dataID, group) == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d listener(s) on %s", want, dataID)
}

// TestUnwatchService_WaitsInflightListenConfig 回归 #170：UnwatchService 必须
// 先等在途 ListenConfig 落地再取消——此前摘去重标记后立刻 StopListenConfig，
// 取消先于注册落地（dataId 粒度取消对未注册监听是 no-op），监听随后落地成
// 孤儿：此后再次 Unwatch（标记已摘）与 Close（快照已不含它）全部 no-op，
// SDK 监听与 goroutine 只能活到进程退出，配置热更回调一直推。
//
// 旧实现红：退订在注册落地前返回（此处第一断言转红）；放行后监听残留。
// 新实现绿：退订停在 watching 在途等待分支，注册落地后再 Stop，监听回收。
func TestUnwatchService_WaitsInflightListenConfig(t *testing.T) {
	store := newGatedConfigStore()
	// 本用例只 gate ListenConfig（在途注册窗口）；Stop 闸预放行——
	// 新实现注册落地后的 StopListenConfig 要立即执行到位。
	close(store.stopRelease)
	c := newTestClient(t, WithConfigStore(store))
	ctx := context.Background()

	// 1. WatchService：去重标记置位，goroutine 卡在 ListenConfig（注册在途）。
	c.WatchService(ctx, "dep-a")
	select {
	case <-store.listenEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("后台注册未进入 ListenConfig（在途窗口未开启）")
	}

	// 2. 退订并发进入：必须等在途注册出结果。
	unwatchErr := make(chan error, 1)
	go func() { unwatchErr <- c.UnwatchService(ctx, "dep-a") }()

	// 核心断言：退订不得先于在途注册落地返回。
	// 旧实现：摘标记即返回（StopListenConfig 落在注册前 = no-op）——此处转红。
	// 新实现：停在 watching 等待分支。
	select {
	case err := <-unwatchErr:
		t.Fatalf("#170 回归：UnwatchService 在在途 ListenConfig 落地前返回（err=%v）——取消先于注册落地成 no-op，监听即将成孤儿", err)
	case <-time.After(300 * time.Millisecond):
		// 新实现预期：仍在等待在途注册。
	}

	// 3. 放行注册 → ListenConfig 落地 → 退订随后 StopListenConfig。
	close(store.listenRelease)
	select {
	case err := <-unwatchErr:
		if err != nil {
			// StopListenConfig 失败以外的原因不允许：等待期间 ctx 未取消
			t.Fatalf("UnwatchService: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("UnwatchService 未返回（疑似死等在途注册）")
	}

	if got := store.ListeningCountFor("dep-a", "DEFAULT_GROUP"); got != 0 {
		t.Fatalf("#170 回归：UnwatchService 返回后 provider 监听 = %d, want 0（监听成孤儿，再次 Unwatch 与 Close 均无法回收）", got)
	}

	// 4. 收尾一致性：再次退订幂等 no-op；Close 后无残留。
	if err := c.UnwatchService(ctx, "dep-a"); err != nil {
		t.Fatalf("再次退订应幂等 no-op, got %v", err)
	}
	c.Close()
	if got := store.ListeningCountFor("dep-a", "DEFAULT_GROUP"); got != 0 {
		t.Fatalf("#170 回归：Close 后 provider 监听 = %d, want 0", got)
	}
}

// TestUnwatchService_WaitsInflightListenConfig_StopFailureRollback 补充钉：
// 在途等待后的退订失败（StopListenConfig 出错）回滚去重标记，允许重试——
// 与落地后退订失败的回滚同口径。
func TestUnwatchService_WaitsInflightListenConfig_StopFailureRollback(t *testing.T) {
	store := newGatedConfigStore()
	// 本用例的 StopListenConfig 失败由 failingConfigStore 注入（短路返回，
	// 不会触达 Stop 闸）；预放行以防重试成功路径被闸挂住。
	close(store.stopRelease)
	c := newTestClient(t, WithConfigStore(store))
	ctx := context.Background()

	c.WatchService(ctx, "dep-a")
	<-store.listenEntered
	close(store.listenRelease)

	// 放行注册后，注入 StopListenConfig 失败（SDK 抖动）。
	failing := &stopFailConfigStore{ConfigStore: store, stopErr: errors.New("sdk down")}
	c.mu.Lock()
	c.configStore = failing
	c.mu.Unlock()
	if err := c.UnwatchService(ctx, "dep-a"); err == nil || !strings.Contains(err.Error(), "dep-a") {
		t.Fatalf("StopListenConfig 失败应上抛并点名服务, got %v", err)
	}
	c.mu.RLock()
	_, stillWatched := c.watchedServices["dep-a"]
	c.mu.RUnlock()
	if !stillWatched {
		t.Fatal("退订失败后去重标记应回滚（允许重试）")
	}

	// 恢复后重试成功，监听回收。
	failing.stopErr = nil
	if err := c.UnwatchService(ctx, "dep-a"); err != nil {
		t.Fatalf("重试 UnwatchService: %v", err)
	}
	if got := store.ListeningCountFor("dep-a", "DEFAULT_GROUP"); got != 0 {
		t.Fatalf("重试后 provider 监听 = %d, want 0", got)
	}
}

// TestWatchService_WaitsInflightUnwatch 回归 #170（订阅方向）：在途退订
// （unwatching 在位）期间并发的 WatchService 必须等退订出结果再登记——
// 否则本方监听落地后可能被在途退订按 dataId 一并拆除，而本地标记已置位
// ⇒ 永不重注册 ⇒ 配置热更静默失联（与 ensureSubscribe 等待 unsubscribing
// 的 #147 同族窗口）。
//
// 旧实现红：WatchService 见标记缺失立即登记，监听被在途退订的 Stop 拆除，
// 标记却仍在位（幽灵标记：看似监听中、实际已死）。新实现绿：退订落地后
// 重新登记，监听真实存活。
func TestWatchService_WaitsInflightUnwatch(t *testing.T) {
	store := newGatedConfigStore()
	c := newTestClient(t, WithConfigStore(store))
	ctx := context.Background()

	// 1. 首次监听完整落地。
	c.WatchService(ctx, "dep-a")
	<-store.listenEntered
	close(store.listenRelease)
	waitListeningCount(t, store, "dep-a", "DEFAULT_GROUP", 1)

	// 2. 退订进入 provider Stop 阶段后挂起（unwatching 在位）。
	unwatchErr := make(chan error, 1)
	go func() { unwatchErr <- c.UnwatchService(ctx, "dep-a") }()
	select {
	case <-store.stopEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("退订未进入 provider Stop 阶段")
	}

	// 3. 并发重听：新实现必须等在途退订出结果再登记（停在 unwatching
	//    等待分支）；旧实现见标记缺失立即登记（listenEntered 的 once 已
	//    消耗，ListenConfig 不再被闸）。
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		c.WatchService(ctx, "dep-a")
	}()
	// 给「不等退订直接登记」的旧实现留出发窗口。
	time.Sleep(200 * time.Millisecond)

	// 4. 放行退订并等两侧收场（新实现的重听在退订落地后苏醒、重新登记）。
	close(store.stopRelease)
	if err := <-unwatchErr; err != nil {
		t.Fatalf("退订失败: %v", err)
	}
	select {
	case <-watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("重听未返回")
	}

	// 5. 核心断言：退订落地后重听的监听必须真实存活（旧实现被在途退订
	//    拆除 ⇒ 监听 0 个而标记在位 ⇒ 热更静默失联）。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && store.ListeningCountFor("dep-a", "DEFAULT_GROUP") == 0 {
		time.Sleep(2 * time.Millisecond)
	}
	if got := store.ListeningCountFor("dep-a", "DEFAULT_GROUP"); got != 1 {
		t.Fatalf("#170 回归：重听后 provider 存活监听 = %d, want 1（监听被在途退订拆除，本地标记却已置位）", got)
	}
	c.mu.RLock()
	watched := c.watchedServices["dep-a"]
	c.mu.RUnlock()
	if !watched {
		t.Fatal("重听后去重标记应在位")
	}
}
