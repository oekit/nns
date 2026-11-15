package nns

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/oekit/nns/client"
	"github.com/oekit/nns/metadata"
	"github.com/oekit/registry"
	nacospkg "github.com/oekit/registry/nacos"
	"github.com/oekit/registry/registrymocks"
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

// listenFailStore 是只在 ListenConfig 上失败的 ConfigStore 替身（#211）：
// GetConfig 正常返回空配置，让 Init 走到监听注册段再失败。
type listenFailStore struct {
	*registrymocks.ConfigStore
}

func (s *listenFailStore) ListenConfig(context.Context, string, string, func(string)) error {
	return errors.New("listen rejected")
}

// TestInit_WatchFailureAggregates 回归（#211）：ListenConfig 注册失败必须让
// Init 返回错误——此前 Init 忽略 WatchService（void + 仅 Warn），启动「成功」
// 而配置热更实际没挂上。聚合错误点名每个监听失败的服务。
func TestInit_WatchFailureAggregates(t *testing.T) {
	sc := client.New(client.WithConfigStore(
		&listenFailStore{ConfigStore: registrymocks.NewConfigStore()}))
	n := &NNSClient{client: sc}

	err := n.Init(context.Background(), "local-svc", []string{"dep-1", "dep-2"})
	if err == nil {
		t.Fatal("Init must fail when config watch registration fails (#211)")
	}
	for _, name := range []string{"local-svc", "dep-1", "dep-2"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("aggregated error must name %s, got: %v", name, err)
		}
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

// TestStaticInstance_OmittedWeightUsable 回归（#61）：门面入口配置静态实例
// 漏写权重字段时，实例不再是「无可用实例」——权重回填为默认 10（与
// registry.Apply / registry.InfoFrom 的 defaultWeight 同口径），下游加权
// 均衡（kitex weighted_balancer 对 weight<=0 整只跳过）可见可用。
func TestStaticInstance_OmittedWeightUsable(t *testing.T) {
	c := New(
		nacospkg.Bootstrap{},
		WithMockMode(true),
		WithStaticInstance("svc", []registry.Instance{
			{IP: "10.0.0.1", Port: 8080}, // 漏配 Weight 字段
		}),
	)

	inst, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("GetInstances error: %v", err)
	}
	if len(inst) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(inst))
	}
	if inst[0].Weight <= 0 {
		t.Errorf("Weight = %v, want > 0 (zero weight would be silently dropped by weighted balancer)", inst[0].Weight)
	}
}
