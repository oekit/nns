package client

import (
	"context"
	"errors"
	"testing"

	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// TestAddStaticInstance_FreshServiceStaysRegistryFirst 回归（nns#128）：
// 运行期给「纯注册中心发现」的服务热加备用地址，不得把服务静默切成静态
// 短路——那会切断注册中心发现（实例上下线、健康摘除、订阅推送全部失效，
// #31 形态在运行时 API 复活）。修复后静态实例以兜底角色叠加：注册中心
// 健康时以注册中心为准。
func TestAddStaticInstance_FreshServiceStaysRegistryFirst(t *testing.T) {
	disc := &registrymocks.Discoverer{Instances: []registry.Instance{{IP: "10.0.0.1", Port: 8080}}}
	c := newTestClient(t, WithDiscoverer(disc))

	c.AddStaticInstance("svc", registry.Instance{IP: "10.9.9.9", Port: 8080})

	instances, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 1 || instances[0].IP != "10.0.0.1" {
		t.Fatalf("runtime static add must not short-circuit registry discovery (nns#128), got %+v", instances)
	}
}

// TestSetStaticInstances_FreshServiceStaysRegistryFirst 同口径覆盖
// SetStaticInstances：运行期覆盖同样不翻转发现路径。
func TestSetStaticInstances_FreshServiceStaysRegistryFirst(t *testing.T) {
	disc := &registrymocks.Discoverer{Instances: []registry.Instance{{IP: "10.0.0.1", Port: 8080}}}
	c := newTestClient(t, WithDiscoverer(disc))

	c.SetStaticInstances("svc", []registry.Instance{{IP: "10.9.9.9", Port: 8080}})

	instances, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 1 || instances[0].IP != "10.0.0.1" {
		t.Fatalf("runtime static set must not short-circuit registry discovery (nns#128), got %+v", instances)
	}
}

// TestAddStaticInstance_FreshService_RegistryDownServesBackup 叠加语义的
// 正面：注册中心故障时，运行期热加的备用地址照常兜底（备用地址要的就是
// 这层保底，不是切断注册中心）。
func TestAddStaticInstance_FreshService_RegistryDownServesBackup(t *testing.T) {
	c := newTestClient(t, WithDiscoverer(&registrymocks.Discoverer{Err: errors.New("nacos down")}))

	c.AddStaticInstance("svc", registry.Instance{IP: "10.9.9.9", Port: 8080})

	instances, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("registry down must fall back to runtime static backup, got error: %v", err)
	}
	if len(instances) != 1 || instances[0].IP != "10.9.9.9" {
		t.Fatalf("expected runtime static backup 10.9.9.9, got %+v", instances)
	}
}

// TestRuntimeStatic_OnShortCircuitService_KeepsShortCircuit 语义跟随的
// 正面：已是短路语义的服务（构造期 WithStaticInstances），运行期写入沿用
// 短路——本地开发/测试的既有用法不变。
func TestRuntimeStatic_OnShortCircuitService_KeepsShortCircuit(t *testing.T) {
	disc := &registrymocks.Discoverer{Instances: []registry.Instance{{IP: "10.0.0.2", Port: 9090}}}
	c := newTestClient(t,
		WithStaticInstances("svc", []registry.Instance{{IP: "10.0.0.1", Port: 8080}}),
		WithDiscoverer(disc),
	)

	c.AddStaticInstance("svc", registry.Instance{IP: "10.0.0.3", Port: 7070})

	instances, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 2 {
		t.Fatalf("short-circuit service keeps short-circuit semantics after runtime add, got %+v", instances)
	}
	if instances[0].IP != "10.0.0.1" || instances[1].IP != "10.0.0.3" {
		t.Fatalf("expected static table 10.0.0.1+10.0.0.3, got %+v", instances)
	}
}

// TestRuntimeStatic_OnFallbackService_KeepsFallback 语义跟随的正面：兜底
// 语义的服务运行期追加，实例进兜底池——注册中心健康时仍以注册中心为准。
func TestRuntimeStatic_OnFallbackService_KeepsFallback(t *testing.T) {
	static := []registry.Instance{{IP: "10.0.0.1", Port: 8080}}

	// 阶段一（注册中心健康）：注册中心优先，兜底池不遮蔽
	disc := &registrymocks.Discoverer{Instances: []registry.Instance{{IP: "10.0.0.2", Port: 9090}}}
	c := newTestClient(t, WithStaticFallback("svc", static), WithDiscoverer(disc))
	c.AddStaticInstance("svc", registry.Instance{IP: "10.0.0.3", Port: 7070})

	instances, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 1 || instances[0].IP != "10.0.0.2" {
		t.Fatalf("registry must keep priority over fallback pool, got %+v", instances)
	}

	// 阶段二（注册中心故障、无缓存）：兜底池两个静态实例都可用
	c2 := newTestClient(t,
		WithStaticFallback("svc", static),
		WithDiscoverer(&registrymocks.Discoverer{Err: errors.New("nacos down")}),
	)
	c2.AddStaticInstance("svc", registry.Instance{IP: "10.0.0.3", Port: 7070})

	instances, err = c2.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("registry down must serve fallback pool, got error: %v", err)
	}
	if len(instances) != 2 {
		t.Fatalf("fallback pool must contain both static entries after runtime add, got %+v", instances)
	}
}

// TestRuntimeStatic_MockMode_StaticStillServed mock 模式钉住：运行期写入
// 的静态实例仍是唯一数据源（mock 下没有注册中心可查，语义标记不影响取值）。
func TestRuntimeStatic_MockMode_StaticStillServed(t *testing.T) {
	c := newTestClient(t, WithMockMode(true))

	c.SetStaticInstances("svc", []registry.Instance{{IP: "10.9.9.9", Port: 8080}})

	instances, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 1 || instances[0].IP != "10.9.9.9" {
		t.Fatalf("mock mode must serve runtime static instances, got %+v", instances)
	}
}
