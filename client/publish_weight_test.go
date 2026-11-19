package client

import (
	"context"
	"testing"

	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// TestPublishWeightNormalized_WatchPush 回归（#243）：watch 推送路径的
// 漏配/零值/负权重实例进缓存前回填 registry.DefaultInstanceWeight，显式
// 正权重原样保留——此前只有静态入口（normalizeStaticInstances，#61）
// 归一，动态推送的 Weight<=0 实例进缓存后 httpx/nns-endpoint 按 1 计权、
// kitex/hertz 经 sdbridge.WeightOrDefault 回填 10，混合栈流量分布不一致。
func TestPublishWeightNormalized_WatchPush(t *testing.T) {
	sub := registrymocks.NewSubscriber()
	c := newTestClient(t, WithSubscriber(sub))

	if err := c.SubscribeService(context.Background(), "svc-push", func([]registry.Instance) {}); err != nil {
		t.Fatalf("SubscribeService: %v", err)
	}

	// provider 推送混合权重实例：漏配（零值）/ 显式零 / 负值 / 显式正
	pushed := []registry.Instance{
		{IP: "10.0.0.1", Port: 8080},
		{IP: "10.0.0.2", Port: 8080, Weight: 0},
		{IP: "10.0.0.3", Port: 8080, Weight: -3},
		{IP: "10.0.0.4", Port: 8080, Weight: 5},
	}
	sub.Push("svc-push", pushed)

	got, err := c.GetInstances(context.Background(), "svc-push")
	if err != nil {
		t.Fatalf("GetInstances: %v", err)
	}
	if len(got) != len(pushed) {
		t.Fatalf("expected %d instances, got %d", len(pushed), len(got))
	}
	for i, inst := range got {
		want := registry.DefaultInstanceWeight
		if i == 3 {
			want = 5 // 显式正权重不被覆盖
		}
		if inst.Weight != want {
			t.Errorf("instance[%d] (%s) Weight = %v, want %v", i, inst.IP, inst.Weight, want)
		}
	}

	// provider 拥有的入参缓冲不被改写（#149 快照契约：归一只作用于本包
	// 私有副本，#243）
	if pushed[0].Weight != 0 {
		t.Errorf("provider input mutated: pushed[0].Weight = %v, want 0", pushed[0].Weight)
	}
}

// TestPublishWeightNormalized_DiscoverPath 回归（#243）：Discover 结果经
// updateCache 发布，与 watch 推送同一发布点归一——漏配权重实例回填
// registry.DefaultInstanceWeight，显式正权重保留。
func TestPublishWeightNormalized_DiscoverPath(t *testing.T) {
	disc := &registrymocks.Discoverer{
		Instances: []registry.Instance{
			{IP: "10.0.0.1", Port: 8080},
			{IP: "10.0.0.2", Port: 8080, Weight: 7},
		},
	}
	c := newTestClient(t, WithDiscoverer(disc))

	got, err := c.GetInstances(context.Background(), "svc-disc")
	if err != nil {
		t.Fatalf("GetInstances: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(got))
	}
	if got[0].Weight != registry.DefaultInstanceWeight {
		t.Errorf("instance[0].Weight = %v, want %v", got[0].Weight, registry.DefaultInstanceWeight)
	}
	if got[1].Weight != 7 {
		t.Errorf("instance[1].Weight = %v, want 7 (explicit weight must not be overwritten)", got[1].Weight)
	}
}
