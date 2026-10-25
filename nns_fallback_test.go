package nns

import (
	"context"
	"errors"
	"testing"

	"github.com/oekit/nns/client"
	"github.com/oekit/registry"
	nacospkg "github.com/oekit/registry/nacos"
)

// TestWithStaticFallback_MockMode 验证门面层的兜底语义选项：
// Mock 模式下静态实例是唯一数据源，兜底语义下同样生效。
func TestWithStaticFallback_MockMode(t *testing.T) {
	c := New(
		nacospkg.Bootstrap{},
		WithMockMode(true),
		WithStaticFallback("dep-svc", []registry.Instance{
			{IP: "10.0.0.1", Port: 8080},
		}),
	)

	inst, err := c.GetInstances(context.Background(), "dep-svc")
	if err != nil {
		t.Fatalf("GetInstances error: %v", err)
	}
	if len(inst) != 1 || inst[0].IP != "10.0.0.1" {
		t.Errorf("expected 10.0.0.1:8080, got %+v", inst)
	}
}

// TestWithFallbackOrder_StaticFirstDefault 验证缺省仍是静态优先（短路），
// 不改变既有调用方的行为。
func TestWithFallbackOrder_StaticFirstDefault(t *testing.T) {
	c := New(
		nacospkg.Bootstrap{},
		WithMockMode(true),
		WithStaticInstance("dep-svc", []registry.Instance{
			{IP: "10.0.0.1", Port: 8080},
		}),
	)

	inst, err := c.GetInstances(context.Background(), "dep-svc")
	if err != nil {
		t.Fatalf("GetInstances error: %v", err)
	}
	if len(inst) != 1 || inst[0].IP != "10.0.0.1" {
		t.Errorf("expected 10.0.0.1:8080, got %+v", inst)
	}
}

// TestWithFallbackOrder_RegistryFirst 验证全局注册中心优先序可配置。
func TestWithFallbackOrder_RegistryFirst(t *testing.T) {
	c := New(
		nacospkg.Bootstrap{},
		WithMockMode(true),
		WithFallbackOrder(client.FallbackRegistryFirst),
		WithStaticInstance("dep-svc", []registry.Instance{
			{IP: "10.0.0.1", Port: 8080},
		}),
	)

	inst, err := c.GetInstances(context.Background(), "dep-svc")
	if err != nil {
		t.Fatalf("GetInstances error: %v", err)
	}
	if len(inst) != 1 || inst[0].IP != "10.0.0.1" {
		t.Errorf("expected static fallback 10.0.0.1:8080, got %+v", inst)
	}
}

// TestStaticFallback_LastCallWins 验证同一服务上重复配置以最后一次调用为准。
func TestStaticFallback_LastCallWins(t *testing.T) {
	c := New(
		nacospkg.Bootstrap{},
		WithMockMode(true),
		WithStaticFallback("dep-svc", []registry.Instance{{IP: "10.0.0.1", Port: 8080}}),
		WithStaticInstance("dep-svc", []registry.Instance{{IP: "10.0.0.2", Port: 9090}}),
	)

	inst, err := c.GetInstances(context.Background(), "dep-svc")
	if err != nil {
		t.Fatalf("GetInstances error: %v", err)
	}
	if len(inst) != 1 || inst[0].IP != "10.0.0.2" {
		t.Errorf("expected last-call-wins 10.0.0.2:9090, got %+v", inst)
	}
}

// TestRegisterAs_NoRegistrar_Error 验证门面在无 registrar 时报错。
func TestRegisterAs_NoRegistrar_Error(t *testing.T) {
	c := New(nacospkg.Bootstrap{ServiceName: "svc"}, WithMockMode(true))

	err := c.RegisterAs(context.Background(), "http-svc", 8080)
	if !errors.Is(err, client.ErrRegistrarUnavailable) {
		t.Fatalf("err = %v, want %v", err, client.ErrRegistrarUnavailable)
	}
}

// TestRegisterAs_EmptyName_Error 验证空服务名报错。
func TestRegisterAs_EmptyName_Error(t *testing.T) {
	c := New(nacospkg.Bootstrap{ServiceName: "svc"}, WithMockMode(true))

	err := c.RegisterAs(context.Background(), "", 8080)
	if !errors.Is(err, client.ErrServiceNameRequired) {
		t.Fatalf("err = %v, want %v", err, client.ErrServiceNameRequired)
	}
}

// TestDeregisterAs_NoRegistrar_NoError 验证无 registrar 时注销不报错。
func TestDeregisterAs_NoRegistrar_NoError(t *testing.T) {
	c := New(nacospkg.Bootstrap{ServiceName: "svc"}, WithMockMode(true))

	if err := c.DeregisterAs(context.Background(), "http-svc", 8080); err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

// TestRegistrarDiscoverer_Accessor 验证门面的逃生口返回 nil（Mock 模式未注入 provider）。
func TestRegistrarDiscoverer_Accessor(t *testing.T) {
	c := New(nacospkg.Bootstrap{ServiceName: "svc"}, WithMockMode(true))

	if c.Registrar() != nil {
		t.Error("Registrar() should be nil in mock mode without provider")
	}
	if c.Discoverer() != nil {
		t.Error("Discoverer() should be nil in mock mode without provider")
	}
}
