package client

import (
	"context"
	"errors"
	"testing"

	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// TestGetAllInstances_StaticShortCircuitAligned 钉住 #129：GetAllInstances
// 必须与 GetInstances 同一套静态语义——短路配置的服务两个入口给出同一实例面
// （此前 GetAllInstances 无视 staticShortCircuits，照样打注册中心）。
func TestGetAllInstances_StaticShortCircuitAligned(t *testing.T) {
	c := newTestClient(t,
		WithStaticInstances("svc", []registry.Instance{{IP: "10.0.0.1", Port: 8080}}),
		WithDiscoverer(&registrymocks.Discoverer{
			AllInstances: []registry.Instance{{IP: "10.0.0.2", Port: 9090}},
		}),
	)

	all, err := c.GetAllInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("GetAllInstances: %v", err)
	}
	if len(all) != 1 || all[0].IP != "10.0.0.1" {
		t.Fatalf("GetAllInstances must follow the static short-circuit like GetInstances, got %+v", all)
	}

	healthy, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("GetInstances: %v", err)
	}
	if len(healthy) != 1 || healthy[0].IP != all[0].IP {
		t.Fatalf("two discovery entries must expose the same instance surface, GetInstances=%+v GetAllInstances=%+v", healthy, all)
	}
}

// TestGetAllInstances_RegistryFailureFallsBack 钉住 #129：注册中心故障时
// GetAllInstances 与 GetInstances 同口径降级（stale/静态兜底），不再一错到底。
func TestGetAllInstances_RegistryFailureFallsBack(t *testing.T) {
	c := newTestClient(t,
		WithStaticFallback("svc", []registry.Instance{{IP: "10.0.0.1", Port: 8080}}),
		WithDiscoverer(&registrymocks.Discoverer{AllErr: errors.New("nacos down")}),
	)

	all, err := c.GetAllInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("GetAllInstances must fall back like GetInstances when registry is down, got error: %v", err)
	}
	if len(all) != 1 || all[0].IP != "10.0.0.1" {
		t.Fatalf("expected static fallback, got %+v", all)
	}
}

// TestGetAllInstances_EmptyResultStaticFallback 钉住 #129：空结果走
// withStaticFallback（与 GetInstances 第 6 级一致）。
func TestGetAllInstances_EmptyResultStaticFallback(t *testing.T) {
	c := newTestClient(t,
		WithStaticFallback("svc", []registry.Instance{{IP: "10.0.0.1", Port: 8080}}),
		WithDiscoverer(&registrymocks.Discoverer{}),
	)

	all, err := c.GetAllInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("GetAllInstances: %v", err)
	}
	if len(all) != 1 || all[0].IP != "10.0.0.1" {
		t.Fatalf("empty registry result must fall back to static, got %+v", all)
	}
}

// TestGetAllInstances_DegradedFallbackOnlyNotShortCircuit 钉住 #129 的
// 降级分支：无 discoverer 时兜底服务的静态实例不当短路返回——新鲜缓存
// （注册中心视角）优先，与 GetInstances 同口径。
func TestGetAllInstances_DegradedFallbackOnlyNotShortCircuit(t *testing.T) {
	c := newTestClient(t,
		WithStaticFallback("svc", []registry.Instance{{IP: "10.0.0.1", Port: 8080}}),
	)
	c.updateCache("svc", []registry.Instance{{IP: "10.0.0.9", Port: 9999}})

	all, err := c.GetAllInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("GetAllInstances: %v", err)
	}
	if len(all) != 1 || all[0].IP != "10.0.0.9" {
		t.Fatalf("fallback-only static must not short-circuit the cache in degraded mode, got %+v", all)
	}

	healthy, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("GetInstances: %v", err)
	}
	if len(healthy) != 1 || healthy[0].IP != all[0].IP {
		t.Fatalf("two discovery entries must expose the same instance surface, GetInstances=%+v GetAllInstances=%+v", healthy, all)
	}
}

// TestGetAllInstances_NoDiscovererNoData_ErrorAligned 钉住 #129：降级分支
// 与 GetInstances 同错误口径——无 discoverer、无缓存、无静态时报错，而不是
// 伪装成功返回空。
func TestGetAllInstances_NoDiscovererNoData_ErrorAligned(t *testing.T) {
	c := newTestClient(t)

	_, allErr := c.GetAllInstances(context.Background(), "svc")
	_, healthyErr := c.GetInstances(context.Background(), "svc")
	if allErr == nil {
		t.Fatalf("GetAllInstances must error like GetInstances (got nil, GetInstances err=%v)", healthyErr)
	}
	if healthyErr == nil {
		t.Fatalf("precondition broken: GetInstances should error in degraded empty mode, got nil")
	}
}
