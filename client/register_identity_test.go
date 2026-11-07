package client

import (
	"context"
	"testing"

	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// TestRegisterDeregister_IPChange_UsesRegisteredIdentity 钉住 #75：
// 注册与注销之间本机 IP 探测结果变化（POD_IP 变更/网卡枚举顺序变化）、
// 且注销漏传 WithGroup/WithCluster 时，Deregister 必须仍用注册时的实例
// 身份（快照）注销——旧实现注销时现算身份（新 IP、空 group/cluster），
// 注册中心按 ip+port+service+group 匹配不到实例，注销静默失效、实例残留。
func TestRegisterDeregister_IPChange_UsesRegisteredIdentity(t *testing.T) {
	t.Setenv("POD_IP", "10.1.1.1")
	reg := &registrymocks.Registrar{}
	c := newTestClient(t, WithRegistrar(reg))

	if err := c.Register(context.Background(), 8080,
		registry.WithGroup("g1"), registry.WithCluster("c1")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// 注册成功后环境变化：IP 变了，注销也不带 group/cluster opts
	t.Setenv("POD_IP", "10.2.2.2")
	if err := c.Deregister(context.Background(), 8080); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	if len(reg.Deregistered) != 1 {
		t.Fatalf("expected 1 deregister, got %d", len(reg.Deregistered))
	}
	d, r := reg.Deregistered[0], reg.Registered[0]
	if d.IP != r.IP {
		t.Errorf("deregistered IP = %q, want registered %q (身份必须来自注册快照)", d.IP, r.IP)
	}
	if d.IP != "10.1.1.1" {
		t.Errorf("deregistered IP = %q, want 10.1.1.1 (不得用注销时现算的 IP)", d.IP)
	}
	if d.Group != "g1" || d.Cluster != "c1" {
		t.Errorf("deregistered group/cluster = %q/%q, want g1/c1 (注册时的维度)", d.Group, d.Cluster)
	}
	if d.Service != r.Service || d.Port != r.Port {
		t.Errorf("deregistered service/port = %q/%d, want %q/%d", d.Service, d.Port, r.Service, r.Port)
	}
}

// TestRegisterAsDeregisterAs_IPChange_UsesRegisteredIdentity 同 #75 场景，
// 走显式服务名入口（RegisterAs/DeregisterAs）。
func TestRegisterAsDeregisterAs_IPChange_UsesRegisteredIdentity(t *testing.T) {
	t.Setenv("POD_IP", "10.1.1.1")
	reg := &registrymocks.Registrar{}
	c := newTestClient(t, WithRegistrar(reg))

	if err := c.RegisterAs(context.Background(), "my-svc-rpc", 9090, registry.WithGroup("biz")); err != nil {
		t.Fatalf("RegisterAs: %v", err)
	}

	t.Setenv("POD_IP", "10.9.9.9")
	if err := c.DeregisterAs(context.Background(), "my-svc-rpc", 9090); err != nil {
		t.Fatalf("DeregisterAs: %v", err)
	}

	if len(reg.Deregistered) != 1 {
		t.Fatalf("expected 1 deregister, got %d", len(reg.Deregistered))
	}
	d := reg.Deregistered[0]
	if d.IP != "10.1.1.1" || d.Group != "biz" {
		t.Errorf("deregistered identity = ip %q group %q, want ip 10.1.1.1 group biz", d.IP, d.Group)
	}
}

// TestRegisterFailure_NotCached 注册失败不得缓存身份：随后 Deregister 走
// 现算路径（注册中心里本就没有该实例，注销语义退回旧行为）。
func TestRegisterFailure_NotCached(t *testing.T) {
	t.Setenv("POD_IP", "10.1.1.1")
	reg := &registrymocks.Registrar{RegErr: errFailingRegistrar}
	c := newTestClient(t, WithRegistrar(reg))

	if err := c.Register(context.Background(), 8080); err == nil {
		t.Fatal("expected register error")
	}

	reg.RegErr = nil // 注册失败后故障恢复，允许注销路径正常工作
	t.Setenv("POD_IP", "10.2.2.2")
	if err := c.Deregister(context.Background(), 8080); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	d := reg.Deregistered[0]
	if d.IP != "10.2.2.2" {
		t.Errorf("deregistered IP = %q, want current 10.2.2.2 (失败注册不留快照)", d.IP)
	}
}

// TestDeregister_NoPriorRegister_BuildsFromCurrentEnv 兜底路径：本 client
// 未注册过（如经 Registrar() 逃生口自行注册的调用方）时，Deregister 按当下
// 环境现算身份（旧行为保持，调用方自行保证维度一致）。
func TestDeregister_NoPriorRegister_BuildsFromCurrentEnv(t *testing.T) {
	t.Setenv("POD_IP", "10.3.3.3")
	reg := &registrymocks.Registrar{}
	c := newTestClient(t, WithRegistrar(reg))

	if err := c.Deregister(context.Background(), 8080); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	d := reg.Deregistered[0]
	if d.IP != "10.3.3.3" || d.Service != "test-service" || d.Port != 8080 {
		t.Errorf("deregistered = %+v, want ip 10.3.3.3 service test-service port 8080", d)
	}
}

// TestDeregister_CacheClearedAfterSuccess 注销成功后清缓存：再次 Deregister
// 不复用旧快照（幂等语义与旧行为一致）。
func TestDeregister_CacheClearedAfterSuccess(t *testing.T) {
	t.Setenv("POD_IP", "10.1.1.1")
	reg := &registrymocks.Registrar{}
	c := newTestClient(t, WithRegistrar(reg))

	if err := c.Register(context.Background(), 8080); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := c.Deregister(context.Background(), 8080); err != nil {
		t.Fatalf("first Deregister: %v", err)
	}

	t.Setenv("POD_IP", "10.4.4.4")
	if err := c.Deregister(context.Background(), 8080); err != nil {
		t.Fatalf("second Deregister: %v", err)
	}
	if got := reg.Deregistered[1].IP; got != "10.4.4.4" {
		t.Errorf("second deregister IP = %q, want 10.4.4.4 (成功注销后快照应清除)", got)
	}
}
