package client

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/yi-nology/ekit-registry/registry"
	"github.com/yi-nology/ekit-registry/registrymocks"
)

// fakeRegistrar 捕获 Register / Deregister 调用（registrymocks.Registrar 别名，
// 保持测试内既有命名）。
type fakeRegistrar = registrymocks.Registrar

// fakeApp 模拟 lifecycle.App,记录 Register / Deregister closer。
type fakeApp struct {
	ctx               context.Context
	cancel            context.CancelFunc
	closers           map[string]func(context.Context) error
	shutdownRequested bool
}

func newFakeApp() *fakeApp {
	ctx, cancel := context.WithCancel(context.Background())
	return &fakeApp{
		ctx:     ctx,
		cancel:  cancel,
		closers: make(map[string]func(context.Context) error),
	}
}

func (f *fakeApp) Context() context.Context { return f.ctx }
func (f *fakeApp) AddResource(name string, closer func(context.Context) error) {
	f.closers[name] = closer
}
func (f *fakeApp) GoBackground(_ string, _ func(context.Context)) {}
func (f *fakeApp) RequestShutdown()                               { f.shutdownRequested = true }

// TestRunWithLifecycle_RegisterAndDeregister 钉住 RunWithLifecycle:
//  1. Register 成功 + lifecycle.AddResource 注册
//  2. 调 closer → Deregister 被调
func TestRunWithLifecycle_RegisterAndDeregister(t *testing.T) {
	fr := &fakeRegistrar{}
	nnsc := New(
		WithRegistrar(fr),
	)
	defer nnsc.Close()

	fa := newFakeApp()
	defer fa.cancel()

	if err := nnsc.RunWithLifecycle(fa, "test-svc", 8080); err != nil {
		t.Fatalf("RunWithLifecycle: %v", err)
	}

	// 验证 Register 被调(1 次,无 Deregister)
	if len(fr.Registered) != 1 {
		t.Errorf("expected 1 Register call, got %d", len(fr.Registered))
	}
	if len(fr.Deregistered) != 0 {
		t.Errorf("expected 0 Deregister before close, got %d", len(fr.Deregistered))
	}

	// 验证 closer 已注册
	closer, ok := fa.closers["test-svc-nns-deregister"]
	if !ok {
		t.Fatal("RunWithLifecycle did not register deregister closer")
	}

	// 调 closer 模拟优雅关闭
	if err := closer(context.Background()); err != nil {
		t.Fatalf("closer returned err: %v", err)
	}

	// 验证 Deregister 被调(1 次)
	if len(fr.Deregistered) != 1 {
		t.Errorf("expected 1 Deregister after close, got %d", len(fr.Deregistered))
	}

	// 验证 instance 信息一致
	if reg := fr.Registered[0]; reg.Port != 8080 {
		t.Errorf("registered Port = %d, want 8080", reg.Port)
	}
	if dereg := fr.Deregistered[0]; dereg.Port != 8080 {
		t.Errorf("deregistered Port = %d, want 8080", dereg.Port)
	}
}

// TestRunWithLifecycle_RegisterErrorPropagated 验证 Register 失败时
// RunWithLifecycle 返回 error,closer 不注册(避免后续 Deregister 失败)。
func TestRunWithLifecycle_RegisterErrorPropagated(t *testing.T) {
	// 用 fail registrar
	failReg := &failingRegistrar{}
	nnsc := New(
		WithRegistrar(failReg),
	)
	defer nnsc.Close()

	fa := newFakeApp()
	defer fa.cancel()

	err := nnsc.RunWithLifecycle(fa, "test-svc", 8080)
	if err == nil {
		t.Fatal("expected error from RunWithLifecycle when Register fails")
	}

	// 验证 closer 没注册
	if _, ok := fa.closers["test-svc-nns-deregister"]; ok {
		t.Error("closer should not be registered when Register fails")
	}
}

// failingRegistrar 总是返回 err
type failingRegistrar struct{}

func (failingRegistrar) Register(_ context.Context, _ registry.Instance) error {
	return errFailingRegistrar
}
func (failingRegistrar) Deregister(_ context.Context, _ registry.Instance) error {
	return errFailingRegistrar
}

var errFailingRegistrar = errors.New("test register failure")

// TestRunWithLifecycle_OptsReused 验证 opts 同时传给 Register 和 Deregister
// (业务应保证 opts 幂等 — 典型 weight/metadata/group)。
func TestRunWithLifecycle_OptsReused(t *testing.T) {
	fr := &fakeRegistrar{}
	nnsc := New(
		WithRegistrar(fr),
	)
	defer nnsc.Close()

	fa := newFakeApp()
	defer fa.cancel()

	opts := []registry.RegisterOptionFunc{
		registry.WithWeight(99),
		registry.WithMetadata(map[string]string{"region": "cn-east"}),
	}
	if err := nnsc.RunWithLifecycle(fa, "test-svc", 8080, opts...); err != nil {
		t.Fatalf("RunWithLifecycle: %v", err)
	}

	closer := fa.closers["test-svc-nns-deregister"]
	if err := closer(context.Background()); err != nil {
		t.Fatalf("closer: %v", err)
	}

	if len(fr.Registered) != 1 || len(fr.Deregistered) != 1 {
		t.Fatalf("expected 1 register + 1 deregister, got %d / %d",
			len(fr.Registered), len(fr.Deregistered))
	}
	if fr.Registered[0].Weight != 99 {
		t.Errorf("registered Weight = %v, want 99", fr.Registered[0].Weight)
	}
	if fr.Deregistered[0].Weight != 99 {
		t.Errorf("deregistered Weight = %v, want 99", fr.Deregistered[0].Weight)
	}
	if fr.Registered[0].Metadata["region"] != "cn-east" {
		t.Errorf("registered metadata region = %v, want cn-east",
			fr.Registered[0].Metadata["region"])
	}
	if fr.Deregistered[0].Metadata["region"] != "cn-east" {
		t.Errorf("deregistered metadata region = %v, want cn-east",
			fr.Deregistered[0].Metadata["region"])
	}
}

// ctxAwareRegistrar 记录 Deregister 收到的 ctx,并对已取消的 ctx 返回错误
// (ctx-aware registrar 的行为模型)。
type ctxAwareRegistrar struct {
	fakeRegistrar
	mu             sync.Mutex
	gotShutdownCtx context.Context
}

func (c *ctxAwareRegistrar) Deregister(ctx context.Context, inst registry.Instance) error {
	c.mu.Lock()
	c.gotShutdownCtx = ctx
	c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.fakeRegistrar.Deregister(ctx, inst)
}

// TestRunWithLifecycle_DeregisterUsesShutdownCtx 钉住 2026-09-05 修复:
// closer 必须收到调用方传入的 shutdown ctx,而不是已取消的 app.Context()。
// 原实现闭包捕获 app ctx,关闭序列里它必然已 cancel — ctx-aware 的
// registrar(etcd/consul 适配器等)注销时会直接失败。
func TestRunWithLifecycle_DeregisterUsesShutdownCtx(t *testing.T) {
	cr := &ctxAwareRegistrar{}
	nnsc := New(
		WithRegistrar(cr),
	)
	defer nnsc.Close()

	fa := newFakeApp()
	defer fa.cancel()

	if err := nnsc.RunWithLifecycle(fa, "ctx-svc", 9090); err != nil {
		t.Fatalf("RunWithLifecycle: %v", err)
	}

	// 模拟关闭序列:app ctx 已取消,bootstrap 传入一个全新的 shutdown ctx
	fa.cancel()
	shutdownCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	closer := fa.closers["ctx-svc-nns-deregister"]
	if err := closer(shutdownCtx); err != nil {
		t.Fatalf("closer with cancelled app ctx should not fail: %v", err)
	}

	cr.mu.Lock()
	got := cr.gotShutdownCtx
	cr.mu.Unlock()
	if got == nil || got.Err() != nil {
		t.Error("Deregister must receive the live shutdown ctx, not the cancelled app ctx")
	}
}
