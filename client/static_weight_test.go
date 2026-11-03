package client

import (
	"context"
	"testing"

	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// TestStaticInstances_ZeroWeightBackfilled 回归（#61）：静态实例权重
// 漏配/零值/负值按注册侧口径（registry.Apply / registry.InfoFrom 的
// defaultWeight=10）回填默认权重——kitex weighted_balancer 对 weight<=0
// 整只跳过，不回填会静默变成「无可用实例」。四个入库点全覆盖。
func TestStaticInstances_ZeroWeightBackfilled(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T) []registry.Instance
	}{
		{
			name: "WithStaticInstances 漏配权重",
			run: func(t *testing.T) []registry.Instance {
				c := newTestClient(t, WithStaticInstances("svc", []registry.Instance{
					{IP: "10.0.0.1", Port: 8080},
				}))
				got, err := c.GetInstances(context.Background(), "svc")
				if err != nil {
					t.Fatalf("GetInstances: %v", err)
				}
				return got
			},
		},
		{
			name: "WithStaticInstances 显式零/负权重",
			run: func(t *testing.T) []registry.Instance {
				c := newTestClient(t, WithStaticInstances("svc", []registry.Instance{
					{IP: "10.0.0.1", Port: 8080, Weight: 0},
					{IP: "10.0.0.2", Port: 8080, Weight: -3},
				}))
				got, err := c.GetInstances(context.Background(), "svc")
				if err != nil {
					t.Fatalf("GetInstances: %v", err)
				}
				return got
			},
		},
		{
			name: "WithStaticFallback 兜底路径",
			run: func(t *testing.T) []registry.Instance {
				c := newTestClient(t,
					WithStaticFallback("svc", []registry.Instance{
						{IP: "10.0.0.1", Port: 8080},
					}),
					WithDiscoverer(&registrymocks.Discoverer{}),
				)
				got, err := c.GetInstances(context.Background(), "svc")
				if err != nil {
					t.Fatalf("GetInstances: %v", err)
				}
				return got
			},
		},
		{
			name: "AddStaticInstance 运行时追加",
			run: func(t *testing.T) []registry.Instance {
				c := newTestClient(t, WithMockMode(true))
				c.AddStaticInstance("svc", registry.Instance{IP: "10.0.0.1", Port: 8080})
				got, err := c.GetInstances(context.Background(), "svc")
				if err != nil {
					t.Fatalf("GetInstances: %v", err)
				}
				return got
			},
		},
		{
			name: "SetStaticInstances 运行时覆盖",
			run: func(t *testing.T) []registry.Instance {
				c := newTestClient(t, WithMockMode(true))
				c.SetStaticInstances("svc", []registry.Instance{
					{IP: "10.0.0.1", Port: 8080},
				})
				got, err := c.GetInstances(context.Background(), "svc")
				if err != nil {
					t.Fatalf("GetInstances: %v", err)
				}
				return got
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.run(t)
			if len(got) == 0 {
				t.Fatal("expected instances, got none")
			}
			for i, inst := range got {
				if inst.Weight != defaultStaticWeight {
					t.Errorf("instance[%d].Weight = %v, want %v", i, inst.Weight, defaultStaticWeight)
				}
			}
		})
	}
}

// TestStaticInstances_ExplicitWeightPreserved 负例：显式正权重不被回填覆盖。
func TestStaticInstances_ExplicitWeightPreserved(t *testing.T) {
	c := newTestClient(t,
		WithStaticInstances("svc", []registry.Instance{
			{IP: "10.0.0.1", Port: 8080, Weight: 5},
			{IP: "10.0.0.2", Port: 8080, Weight: 20},
			{IP: "10.0.0.3", Port: 8080, Weight: 0.5},
		}),
	)

	got, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("GetInstances: %v", err)
	}
	want := []float64{5, 20, 0.5}
	if len(got) != len(want) {
		t.Fatalf("expected %d instances, got %d", len(want), len(got))
	}
	for i, w := range want {
		if got[i].Weight != w {
			t.Errorf("instance[%d].Weight = %v, want %v (explicit weight must not be overwritten)", i, got[i].Weight, w)
		}
	}
}

// TestStaticInstances_OmittedWeightStillUsable 回归（#61）：配置只写
// IP/Port、漏配权重字段时，实例不再是「无可用实例」——权重回填为默认 10，
// 对下游加权均衡（weight<=0 整只跳过）可见可用。
func TestStaticInstances_OmittedWeightStillUsable(t *testing.T) {
	c := newTestClient(t, WithStaticInstances("svc", []registry.Instance{
		{IP: "10.0.0.1", Port: 8080},
	}))

	got, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("GetInstances: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(got))
	}
	if got[0].Weight <= 0 {
		t.Errorf("Weight = %v, want > 0 (weight<=0 instances are dropped by weighted balancer)", got[0].Weight)
	}
}
