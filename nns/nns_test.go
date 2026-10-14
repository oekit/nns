package nns

import (
	"context"
	"testing"

	nacospkg "github.com/yi-nology/ekit-registry/nacos"
	"github.com/yi-nology/ekit-nns/nns/metadata"
	"github.com/yi-nology/ekit-registry/registry"
)

// TestNew_ReturnsIndependentInstances 是 NNS 状态治理的核心验收测试：
// 验证两次 New 返回相互独立的实例，配置互不影响。
// 这直接对应 ARCHITECTURE_ROADMAP 阶段一的验收标准。
func TestNew_ReturnsIndependentInstances(t *testing.T) {
	a := New(
		nacospkg.Bootstrap{},
		WithMockMode(true),
		WithStaticInstance("target", []registry.Instance{
			{IP: "10.0.0.1", Port: 8080},
		}),
	)
	b := New(
		nacospkg.Bootstrap{},
		WithMockMode(true),
		WithStaticInstance("target", []registry.Instance{
			{IP: "10.0.0.2", Port: 9090},
		}),
	)

	if a == b {
		t.Fatal("expected different NNSClient pointer instances")
	}

	instA, err := a.GetInstances(context.Background(), "target")
	if err != nil {
		t.Fatalf("A GetInstances error: %v", err)
	}
	if len(instA) != 1 || instA[0].IP != "10.0.0.1" {
		t.Errorf("A expected 10.0.0.1:8080, got %+v", instA)
	}

	instB, err := b.GetInstances(context.Background(), "target")
	if err != nil {
		t.Fatalf("B GetInstances error: %v", err)
	}
	if len(instB) != 1 || instB[0].IP != "10.0.0.2" {
		t.Errorf("B expected 10.0.0.2:9090 (must not be polluted by A), got %+v", instB)
	}
}

// TestNew_StaticInstanceMutationIsolated 验证通过 AddStaticInstance 修改实例 A
// 不会影响实例 B 的状态。
func TestNew_StaticInstanceMutationIsolated(t *testing.T) {
	a := New(nacospkg.Bootstrap{ServiceName: "svc-a"}, WithMockMode(true))
	b := New(nacospkg.Bootstrap{ServiceName: "svc-b"}, WithMockMode(true))

	a.AddStaticInstance("shared-svc", registry.Instance{IP: "10.0.0.1", Port: 8080})

	instB, err := b.GetInstances(context.Background(), "shared-svc")
	if err != nil {
		t.Fatalf("B GetInstances error: %v", err)
	}
	if len(instB) != 0 {
		t.Errorf("B should have no instances for shared-svc (mutation leaked from A), got %d", len(instB))
	}

	instA, _ := a.GetInstances(context.Background(), "shared-svc")
	if len(instA) != 1 {
		t.Errorf("A expected 1 instance, got %d", len(instA))
	}
}

func TestInit_MockMode_NoError(t *testing.T) {
	c := New(
		nacospkg.Bootstrap{},
		WithMockMode(true),
		WithLocalServiceConfig("my-service", &metadata.ServiceBasic{
			ConnectionConfig: metadata.ConnectionConfig{
				ConnTimeout: 3000,
				ReadTimeout: 9000,
			},
		}),
	)

	err := c.Init(context.Background(), "my-service", nil)
	if err != nil {
		t.Fatalf("Init error: %v", err)
	}
}

func TestInit_WithDepServices_MockMode(t *testing.T) {
	c := New(
		nacospkg.Bootstrap{},
		WithMockMode(true),
		WithLocalServiceConfig("my-service", &metadata.ServiceBasic{}),
		WithLocalServiceConfig("dep-1", &metadata.ServiceBasic{}),
	)

	err := c.Init(context.Background(), "my-service", []string{"dep-1"})
	if err != nil {
		t.Fatalf("Init error: %v", err)
	}
}

func TestSetStaticInstances(t *testing.T) {
	c := New(nacospkg.Bootstrap{ServiceName: "svc"}, WithMockMode(true))

	c.SetStaticInstances("override-svc", []registry.Instance{
		{IP: "10.0.0.6", Port: 6060},
		{IP: "10.0.0.7", Port: 7070},
	})

	inst, err := c.GetInstances(context.Background(), "override-svc")
	if err != nil {
		t.Fatalf("GetInstances error: %v", err)
	}
	if len(inst) != 2 {
		t.Errorf("expected 2 instances, got %d", len(inst))
	}
}

func TestGetAllInstances_MockMode(t *testing.T) {
	c := New(
		nacospkg.Bootstrap{},
		WithMockMode(true),
		WithStaticInstance("svc-all", []registry.Instance{
			{IP: "10.0.0.1", Port: 8080},
		}),
	)

	inst, err := c.GetAllInstances(context.Background(), "svc-all")
	if err != nil {
		t.Fatalf("GetAllInstances error: %v", err)
	}
	if len(inst) != 1 {
		t.Errorf("expected 1 instance, got %d", len(inst))
	}
}

func TestMakeHTTPEndpoint(t *testing.T) {
	c := New(
		nacospkg.Bootstrap{},
		WithMockMode(true),
		WithStaticInstance("http-svc", []registry.Instance{
			{IP: "10.0.0.1", Port: 8080},
		}),
	)
	defer c.Close()

	ep, err := c.MakeHTTPEndpoint("http-svc")
	if err != nil {
		t.Fatalf("MakeHTTPEndpoint error: %v", err)
	}
	if ep == nil {
		t.Fatal("expected non-nil endpoint")
	}
}

func TestServiceClient(t *testing.T) {
	c := New(nacospkg.Bootstrap{ServiceName: "svc"}, WithMockMode(true))

	sc := c.ServiceClient()
	if sc == nil {
		t.Fatal("expected non-nil ServiceClient")
	}
	// 验证两次调用返回相同的内部实例（同一 NNSClient 的 ServiceClient 应是稳定的）。
	if sc != c.ServiceClient() {
		t.Error("ServiceClient() should return a stable reference to the same instance")
	}
}

func TestClose_NoPanic(t *testing.T) {
	c := New(nacospkg.Bootstrap{ServiceName: "svc"}, WithMockMode(true))
	c.Close()
}

func TestClose_NilSafe(t *testing.T) {
	c := New(nacospkg.Bootstrap{ServiceName: "svc"}, WithMockMode(true))
	c.Close()
	c.Close()
}
