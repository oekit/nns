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

// raceProbeSubscriber 是 #165 数据竞争探针的订阅器：记录注册的派发回调，
// 可注入「Subscribe 必然失败」——失败触发 rollbackEntryLocked 的切片搬移
// （修复前为原地 append 搬移），与推送扇出的锁外遍历构成 -race 实锤的
// 写读竞争。
type raceProbeSubscriber struct {
	mu     sync.Mutex
	lastCB func([]registry.Instance)
	fail   atomic.Bool
}

func (s *raceProbeSubscriber) Subscribe(_ context.Context, _ string, cb func([]registry.Instance)) error {
	s.mu.Lock()
	s.lastCB = cb
	s.mu.Unlock()
	if s.fail.Load() {
		// 拉长「追加 → 注册失败 → 回滚」窗口：并发订阅方的条目得以在列表里
		// 重叠（列表长度 > 1 时回滚搬移才真正移动元素），单点回归（只还原
		// 追加或只还原回滚）都能被 -race 抓到。
		time.Sleep(time.Millisecond)
		return errors.New("raceprobe: injected subscribe failure")
	}
	return nil
}

func (s *raceProbeSubscriber) Unsubscribe(context.Context, string) error { return nil }

// TestDispatchFanout_ConcurrentRollback_NoRace 复现并钉住 #165：推送扇出
// （newDispatch 在 RUnlock 之后遍历回调切片）与锁内的订阅追加/失败回滚
// 原地搬移之间无 happens-before 边 ⇒ 数据竞争（issue 实测 -race 两处
// WARNING：watch.go SubscribeService 追加 / rollbackEntryLocked 搬移，
// 对 newDispatch 锁外遍历）。
//
// 修法采用 copy-on-write：追加（SubscribeService）与回滚
// （rollbackEntryLocked）都发布全新切片，锁外快照遍历读的 backing array
// 永不再被写。变异性负对照：把 SubscribeService 还原为原地 append、
// rollbackEntryLocked 还原为 append(list[:i], list[i+1:]...) 搬移后，本
// 用例在 -race 下转红（DATA RACE 复现）。
//
// 压测形态与 issue 实测探针一致：一轮成功订阅拿到派发回调 → 注入恒失败
// 订阅 → 2 个 goroutine 持续模拟 provider 推送 × 300 轮 4 并发失败订阅
// （每轮必触发「锁内追加 → 注册失败 → 回滚搬移」）。现有用例全绿盖不住
// 本竞争——没有任何一条把「推送扇出」与「订阅失败回滚」真正并发起来。
func TestDispatchFanout_ConcurrentRollback_NoRace(t *testing.T) {
	sub := &raceProbeSubscriber{}
	c := newTestClient(t, WithSubscriber(sub))

	ctx := context.Background()
	// 一轮成功订阅：拿到 provider 侧派发回调（推送扇出入口）
	if err := c.SubscribeService(ctx, "svc-fanout", func([]registry.Instance) {}); err != nil {
		t.Fatalf("setup subscribe: %v", err)
	}
	sub.mu.Lock()
	dispatch := sub.lastCB
	sub.mu.Unlock()
	if dispatch == nil {
		t.Fatal("dispatch callback not registered")
	}

	// 退订清态（与 issue 探针同款）：清掉 subscribedServices 去重标记——
	// 否则后续 SubscribeService 全走去重路径，根本不会触发「注册失败回滚」
	if err := c.UnsubscribeService(ctx, "svc-fanout"); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}

	// 注入恒失败订阅：此后每轮 SubscribeService 必走「追加 → 注册失败 → 回滚」
	sub.fail.Store(true)

	// 推送侧：2 个 goroutine 持续扇出（锁外遍历回调切片）
	stop := make(chan struct{})
	var pushWG sync.WaitGroup
	for i := 0; i < 2; i++ {
		pushWG.Add(1)
		go func() {
			defer pushWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
					dispatch(nil)
				}
			}
		}()
	}

	// 订阅侧：300 轮 × 8 并发失败订阅（锁内追加 + 回滚搬移）
	for round := 0; round < 300; round++ {
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
				_ = c.SubscribeService(sctx, "svc-fanout", func([]registry.Instance) {})
			}()
		}
		wg.Wait()
	}
	close(stop)
	pushWG.Wait()
}
