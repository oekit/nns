package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// TestGetAllInstances_ShortCircuitServiceMatchesGetInstances 回归（nns#129）：
// 短路语义的服务，GetAllInstances 必须与 GetInstances 同面（返回静态表、
// 不打注册中心）——此前本入口不看短路判定，同一配置两个发现入口给出不同
// 实例面（管理台列表与调用路由不一致）。
func TestGetAllInstances_ShortCircuitServiceMatchesGetInstances(t *testing.T) {
	disc := &registrymocks.Discoverer{
		Instances:    []registry.Instance{{IP: "10.0.0.2", Port: 9090}},
		AllInstances: []registry.Instance{{IP: "10.0.0.2", Port: 9090}},
	}
	c := newTestClient(t,
		WithStaticInstances("svc", []registry.Instance{{IP: "10.0.0.1", Port: 8080}}),
		WithDiscoverer(disc),
	)

	all, err := c.GetAllInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	route, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(all) != 1 || all[0].IP != "10.0.0.1" {
		t.Fatalf("GetAllInstances must honor static short-circuit like GetInstances (nns#129), got %+v", all)
	}
	if len(all) != len(route) || all[0].IP != route[0].IP {
		t.Fatalf("two discovery entries must agree on the same instance face, all=%v route=%v", all, route)
	}
}

// TestGetAllInstances_DiscoverFailure_FallsBackLikeGetInstances 回归（nns#129）：
// 注册中心故障时 GetAllInstances 与 GetInstances 同口径降级（兜底静态），
// 不再硬报错——「业务活着、面板红了」的错误口径分裂。
func TestGetAllInstances_DiscoverFailure_FallsBackLikeGetInstances(t *testing.T) {
	static := []registry.Instance{{IP: "10.0.0.1", Port: 8080}}
	disc := &registrymocks.Discoverer{Err: errors.New("nacos down"), AllErr: errors.New("nacos down")}
	c := newTestClient(t, WithStaticFallback("svc", static), WithDiscoverer(disc))

	all, err := c.GetAllInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("GetAllInstances must degrade like GetInstances on registry failure (nns#129), got: %v", err)
	}
	route, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(all) != 1 || all[0].IP != "10.0.0.1" {
		t.Fatalf("expected static fallback 10.0.0.1, got %+v", all)
	}
	if len(all) != len(route) || all[0].IP != route[0].IP {
		t.Fatalf("two discovery entries must share the same failover face, all=%v route=%v", all, route)
	}
}

// TestGetAllInstances_DiscoverFailure_StaleCacheFallback 同口径覆盖 stale
// 缓存层：DiscoverAll 失败且无兜底静态时，stale 缓存兜底（宁要过期数据，
// 不硬报错）。
func TestGetAllInstances_DiscoverFailure_StaleCacheFallback(t *testing.T) {
	disc := &registrymocks.Discoverer{AllErr: errors.New("nacos down")}
	c := newTestClient(t, WithDiscoverer(disc), WithCacheExpiry(time.Nanosecond))
	c.updateCache("svc", []registry.Instance{{IP: "10.0.0.7", Port: 7070}})
	time.Sleep(2 * time.Millisecond) // 让缓存过期成 stale

	all, err := c.GetAllInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("stale cache must backstop DiscoverAll failure, got: %v", err)
	}
	if len(all) != 1 || all[0].IP != "10.0.0.7" {
		t.Fatalf("expected stale cached instance 10.0.0.7, got %+v", all)
	}
}

// TestGetAllInstances_EmptyRegistry_FallsBackToStatic 注册中心返回空列表
// → 兜底静态（与 GetInstances 第 6 级同口径，#129）。
func TestGetAllInstances_EmptyRegistry_FallsBackToStatic(t *testing.T) {
	static := []registry.Instance{{IP: "10.0.0.1", Port: 8080}}
	c := newTestClient(t,
		WithStaticFallback("svc", static),
		WithDiscoverer(&registrymocks.Discoverer{AllInstances: []registry.Instance{}}),
	)

	all, err := c.GetAllInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(all) != 1 || all[0].IP != "10.0.0.1" {
		t.Fatalf("empty registry result must fall back to static like GetInstances (nns#129), got %+v", all)
	}
}

// TestGetAllInstances_Degraded_FallbackStaticNotServedAsShortCircuit 回归
// （nns#129）的降级分支面：无 discoverer 且有新鲜缓存时，兜底语义的服务与
// GetInstances 同面（返回缓存）——此前 getInstancesFromStaticOrCache 无视
// fallbackOnly，把兜底静态当短路返回（静态遮蔽缓存）。
func TestGetAllInstances_Degraded_FallbackStaticNotServedAsShortCircuit(t *testing.T) {
	static := []registry.Instance{{IP: "10.0.0.1", Port: 8080}}
	c := newTestClient(t, WithStaticFallback("svc", static)) // 无 discoverer（降级态）
	c.updateCache("svc", []registry.Instance{{IP: "10.0.0.5", Port: 5050}})

	all, err := c.GetAllInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	route, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(all) != 1 || all[0].IP != "10.0.0.5" {
		t.Fatalf("degraded fallback-marked service must serve cache like GetInstances (nns#129), got %+v", all)
	}
	if len(all) != len(route) || all[0].IP != route[0].IP {
		t.Fatalf("two discovery entries must agree in degraded state, all=%v route=%v", all, route)
	}
}

// TestGetAllInstances_Degraded_NothingAvailable_Errors 降级态且缓存/兜底
// 全空时显式报错（与 GetInstances 的「no discoverer available」口径一致，
// 不再返回「空列表 + nil」假成功）。
func TestGetAllInstances_Degraded_NothingAvailable_Errors(t *testing.T) {
	c := newTestClient(t) // 无 discoverer、无静态、无缓存

	all, err := c.GetAllInstances(context.Background(), "svc")
	if err == nil {
		t.Fatalf("degraded with nothing available must error like GetInstances (nns#129), got %+v", all)
	}
}
