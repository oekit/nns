package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/oekit/registry"
)

// ctxRecordingSubscriber 记录 Unsubscribe 收到的 ctx，并让注销失败。
type ctxRecordingSubscriber struct {
	mu       sync.Mutex
	deadline bool
	calls    int
}

func (s *ctxRecordingSubscriber) Subscribe(ctx context.Context, service string, cb func([]registry.Instance)) error {
	return nil
}

func (s *ctxRecordingSubscriber) Unsubscribe(ctx context.Context, service string) error {
	s.mu.Lock()
	s.calls++
	_, s.deadline = ctx.Deadline()
	s.mu.Unlock()
	return errors.New("unsubscribe boom")
}

func (s *ctxRecordingSubscriber) state() (deadline bool, calls int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deadline, s.calls
}

// TestCloseWithContext_BoundedAndSurfacesError 回归测试：关闭流程的 ctx 必须是
// **有界的**，且关闭错误不得被吞掉。
//
// 缺陷成因：Close 内部用 context.Background() 调 SDK 的 Unsubscribe /
// StopListenConfig ⇒ SDK 不响应时 Close **永久挂住**（调用方无从得知卡在哪
// 一步）；且失败只记日志、调用方拿不到任何错误。
//
// 修法：新增 CloseWithContext(ctx)——ctx 透传给调用方，Close 入口走默认 5s
// 上限；关闭错误原样返回（Close 入口仍只记日志，因为它没有返回值可给）。
func TestCloseWithContext_BoundedAndSurfacesError(t *testing.T) {
	sub := &ctxRecordingSubscriber{}
	c := New(WithSubscriber(sub))

	if err := c.SubscribeService(context.Background(), "svc", func([]registry.Instance) {}); err != nil {
		t.Fatalf("SubscribeService: %v", err)
	}

	err := c.CloseWithContext(context.Background())
	if err == nil {
		t.Fatal("关闭失败（unsubscribe boom）应被返回，而不是被吞掉")
	}

	deadline, calls := sub.state()
	if calls == 0 {
		t.Fatal("Unsubscribe 未被调用")
	}
	if !deadline {
		t.Fatal("Unsubscribe 收到的 ctx **没有 deadline**：关闭流程仍是无界的（SDK 不响应时永久挂住）")
	}
}

// TestClose_UsesBoundedContext Close（无 ctx 入口）也必须走有界 ctx。
func TestClose_UsesBoundedContext(t *testing.T) {
	sub := &ctxRecordingSubscriber{}
	c := New(WithSubscriber(sub))
	if err := c.SubscribeService(context.Background(), "svc", func([]registry.Instance) {}); err != nil {
		t.Fatalf("SubscribeService: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Close()
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close 永久挂住：内部 ctx 无界")
	}

	deadline, _ := sub.state()
	if !deadline {
		t.Fatal("Close 入口也应使用有界 ctx（默认上限）")
	}
}

// TestClose_Idempotent Close 与 CloseWithContext 共享同一个 sync.Once：
// 重复调用只有第一次真正执行（仓库硬规则：Close 幂等）。
func TestClose_Idempotent(t *testing.T) {
	sub := &ctxRecordingSubscriber{}
	c := New(WithSubscriber(sub))
	if err := c.SubscribeService(context.Background(), "svc", func([]registry.Instance) {}); err != nil {
		t.Fatalf("SubscribeService: %v", err)
	}

	_ = c.CloseWithContext(context.Background())
	_ = c.CloseWithContext(context.Background())
	c.Close()

	_, calls := sub.state()
	if calls != 1 {
		t.Fatalf("Close 重复调用应只执行一次，实得 %d 次", calls)
	}
}
