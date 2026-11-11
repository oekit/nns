package client

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// TestUnwatchService_StopsSingleListener 回归 #148：WatchService 此前没有
// 单独的退订入口——配置监听只能随 Close 全量终结，动态下线的依赖服务的
// 监听与 goroutine 只能积累到进程 Close。UnwatchService 摘标记 +
// StopListenConfig（dataId 粒度），单个服务可独立回收，且取消后可重听。
func TestUnwatchService_StopsSingleListener(t *testing.T) {
	store := registrymocks.NewConfigStore()
	c := newTestClient(t, WithConfigStore(store))
	ctx := context.Background()

	c.WatchService(ctx, "dep-a")
	c.WatchService(ctx, "dep-b")
	waitListening(t, store, "dep-a", "DEFAULT_GROUP")
	waitListening(t, store, "dep-b", "DEFAULT_GROUP")

	if err := c.UnwatchService(ctx, "dep-a"); err != nil {
		t.Fatalf("UnwatchService: %v", err)
	}
	if store.ListeningCountFor("dep-a", "DEFAULT_GROUP") != 0 {
		t.Fatal("UnwatchService 后 dep-a 的 provider 监听应已取消")
	}
	if store.ListeningCountFor("dep-b", "DEFAULT_GROUP") != 1 {
		t.Fatal("UnwatchService 只应回收目标服务，dep-b 监听不得被牵连")
	}

	c.mu.RLock()
	_, stillWatched := c.watchedServices["dep-a"]
	c.mu.RUnlock()
	if stillWatched {
		t.Fatal("UnwatchService 后去重标记应已摘除")
	}

	// 取消后可重新 WatchService（对称生命周期）
	c.WatchService(ctx, "dep-a")
	waitListening(t, store, "dep-a", "DEFAULT_GROUP")

	// 未监听过的服务：幂等 no-op
	if err := c.UnwatchService(ctx, "never-watched"); err != nil {
		t.Fatalf("未监听服务应为幂等 no-op, got %v", err)
	}
}

// TestUnwatchService_StopFailure_RollbackMark StopListenConfig 失败时回滚
// 去重标记（与 WatchService 注册失败的回滚同口径），允许重试。
func TestUnwatchService_StopFailure_RollbackMark(t *testing.T) {
	store := registrymocks.NewConfigStore()
	c := newTestClient(t, WithConfigStore(store))
	ctx := context.Background()

	c.WatchService(ctx, "dep-a")
	waitListening(t, store, "dep-a", "DEFAULT_GROUP")

	// provider 退订失败（SDK 抖动）：标记回滚，可重试
	failing := &failingConfigStore{ConfigStore: store, stopErr: errors.New("sdk down")}
	c.mu.Lock()
	c.configStore = failing
	c.mu.Unlock()
	if err := c.UnwatchService(ctx, "dep-a"); err == nil {
		t.Fatal("StopListenConfig 失败应上抛错误")
	} else if !strings.Contains(err.Error(), "dep-a") {
		t.Fatalf("错误应点名服务, got %v", err)
	}
	c.mu.RLock()
	_, stillWatched := c.watchedServices["dep-a"]
	c.mu.RUnlock()
	if !stillWatched {
		t.Fatal("退订失败后去重标记应回滚（允许重试）")
	}

	// 恢复后重试成功
	failing.stopErr = nil
	if err := c.UnwatchService(ctx, "dep-a"); err != nil {
		t.Fatalf("重试 UnwatchService: %v", err)
	}
	if store.ListeningCountFor("dep-a", "DEFAULT_GROUP") != 0 {
		t.Fatal("重试后监听应已取消")
	}
}

// failingConfigStore 包装 mock，可对 StopListenConfig 注入失败。
type failingConfigStore struct {
	*registrymocks.ConfigStore
	stopErr error
}

func (f *failingConfigStore) StopListenConfig(ctx context.Context, dataID, group string) error {
	if f.stopErr != nil {
		return f.stopErr
	}
	return f.ConfigStore.StopListenConfig(ctx, dataID, group)
}

// TestUnwatchService_MockModeNoOp Mock 模式 / 无 ConfigStore 时 no-op
// （与 WatchService 的 no-op 对称）。
func TestUnwatchService_MockModeNoOp(t *testing.T) {
	c := newTestClient(t, WithMockMode(true))
	if err := c.UnwatchService(context.Background(), "svc"); err != nil {
		t.Fatalf("mock 模式 UnwatchService 应 no-op nil, got %v", err)
	}
	c2 := newTestClient(t)
	if err := c2.UnwatchService(context.Background(), "svc"); err != nil {
		t.Fatalf("无 ConfigStore 时 UnwatchService 应 no-op nil, got %v", err)
	}
}

// 编译期守卫：确认 mock ConfigStore 实现 registry.ConfigStore。
var _ registry.ConfigStore = (*failingConfigStore)(nil)
