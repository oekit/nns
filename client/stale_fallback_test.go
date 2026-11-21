package client

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// TestGetInstances_NoDiscoverer_StaleCacheFallback 回归 #150：无 Discoverer
// 但 stale 缓存可用时，GetInstances 必须返回 stale 实例而非硬报错。
//
// subscriber-only 客户端（Subscribe 推送填缓存、不需要 Discoverer）是
// client/doc.go 明确支持的注入形态：缓存一过期（新鲜缓存第 3 级视为未命中）
// 就撞上「无 Discoverer」分支——此前该分支跳过 stale 缓存直接硬报错，与
// 「Discover 失败」形态（有 stale 兜底 + WARN 留痕）同一故障两种表现。
// 变异性负对照：还原分支为「直接 staticFallbackPool → 报错」后本用例转红。
func TestGetInstances_NoDiscoverer_StaleCacheFallback(t *testing.T) {
	sub := registrymocks.NewSubscriber()
	c := newTestClient(t, WithSubscriber(sub), WithCacheExpiry(50*time.Millisecond))

	// subscriber-only 形态：缓存由 Subscribe 推送填充
	if err := c.SubscribeService(context.Background(), "svc-stale", func([]registry.Instance) {}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	sub.Push("svc-stale", []registry.Instance{
		{Service: "svc-stale", IP: "10.0.0.9", Port: 9090},
	})

	// 等缓存过期：新鲜缓存第 3 级视为未命中，落入「无 Discoverer」分支
	time.Sleep(80 * time.Millisecond)

	instances, err := c.GetInstances(context.Background(), "svc-stale")
	if err != nil {
		t.Fatalf("#150 regression: stale cache must be served when no discoverer is available, got error: %v", err)
	}
	if len(instances) != 1 || instances[0].IP != "10.0.0.9" {
		t.Fatalf("expected stale instance 10.0.0.9:9090, got %+v", instances)
	}
}

// TestGetAllInstances_NoDiscoverer_StaleCacheFallback 同 #150，全量视图入口
// （与 GetInstances 走同一个「无 Discoverer」分支，口径必须一致）。
func TestGetAllInstances_NoDiscoverer_StaleCacheFallback(t *testing.T) {
	sub := registrymocks.NewSubscriber()
	c := newTestClient(t, WithSubscriber(sub), WithCacheExpiry(50*time.Millisecond))

	if err := c.SubscribeService(context.Background(), "svc-stale-all", func([]registry.Instance) {}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	sub.Push("svc-stale-all", []registry.Instance{
		{Service: "svc-stale-all", IP: "10.0.0.8", Port: 8080},
	})
	time.Sleep(80 * time.Millisecond)

	instances, err := c.GetAllInstances(context.Background(), "svc-stale-all")
	if err != nil {
		t.Fatalf("#150 regression: GetAllInstances must serve stale cache without discoverer, got error: %v", err)
	}
	if len(instances) != 1 || instances[0].IP != "10.0.0.8" {
		t.Fatalf("expected stale instance 10.0.0.8:8080, got %+v", instances)
	}
}

// TestGetInstances_NoDiscoverer_StaleCacheOverStaticFallback 钉 #150 的降级
// 优先序：stale 缓存先于兜底静态——与「Discover 失败」路径（第 5/6 级）
// 同口径。注册中心视图（即使过期）优先于静态地址，是「注册中心优先、
// 静态地址兜底」生产形态在降级态的延续。
func TestGetInstances_NoDiscoverer_StaleCacheOverStaticFallback(t *testing.T) {
	c := newTestClient(t,
		WithStaticFallback("svc-pre", []registry.Instance{{Service: "svc-pre", IP: "10.9.9.9", Port: 9999}}),
		WithCacheExpiry(50*time.Millisecond),
	)
	c.updateCache("svc-pre", []registry.Instance{{Service: "svc-pre", IP: "10.0.0.1", Port: 80}})
	time.Sleep(80 * time.Millisecond)

	instances, err := c.GetInstances(context.Background(), "svc-pre")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 1 || instances[0].IP != "10.0.0.1" {
		t.Fatalf("stale cache must win over static fallback (same precedence as Discover-failure path), got %+v", instances)
	}
}

// TestGetInstances_DiscoverFailure_EmptyStaleCache_StaticFallback 回归 #251：
// 注册中心先返回空列表（缓存发布为非 nil 空切片）、随后故障 → stale 缓存
// 分支不得返回空列表 + nil error 跳过静态兜底。
//
// 此前两条降级分支只判 cached != nil 就 return cached：非 nil 空快照让
// 「注册中心优先 + 静态地址兜底」在最需要它的冷启动/全实例不健康场景失效
// （nns-endpoint 收到 ErrNoInstances），且同一配置在缓存过期前后表现不一致
// ——新鲜缓存路径恒过 withStaticFallback。变异性负对照：去掉降级分支的
// withStaticFallback 后本用例转红（返回空列表）。
func TestGetInstances_DiscoverFailure_EmptyStaleCache_StaticFallback(t *testing.T) {
	disc := &registrymocks.Discoverer{Err: errors.New("nacos down")}
	c := newTestClient(t,
		WithStaticFallback("svc-empty", []registry.Instance{{Service: "svc-empty", IP: "10.9.9.9", Port: 9999}}),
		WithDiscoverer(disc),
		WithCacheExpiry(50*time.Millisecond),
	)
	// 注册中心曾返回空列表：缓存条目非 nil、长度 0
	c.updateCache("svc-empty", []registry.Instance{})
	time.Sleep(80 * time.Millisecond) // 成为 stale 缓存

	instances, err := c.GetInstances(context.Background(), "svc-empty")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 1 || instances[0].IP != "10.9.9.9" {
		t.Fatalf("#251 regression: empty stale snapshot must enter static fallback, got %+v", instances)
	}
}

// TestGetInstances_NoDiscoverer_EmptyStaleCache_StaticFallback 同 #251，钉
// 「无 Discoverer」降级分支：空 stale 快照同样按无实例进入兜底链（与
// Discover 失败分支同口径）。
func TestGetInstances_NoDiscoverer_EmptyStaleCache_StaticFallback(t *testing.T) {
	c := newTestClient(t,
		WithStaticFallback("svc-empty-nd", []registry.Instance{{Service: "svc-empty-nd", IP: "10.8.8.8", Port: 8888}}),
		WithCacheExpiry(50*time.Millisecond),
	)
	c.updateCache("svc-empty-nd", []registry.Instance{})
	time.Sleep(80 * time.Millisecond)

	instances, err := c.GetInstances(context.Background(), "svc-empty-nd")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 1 || instances[0].IP != "10.8.8.8" {
		t.Fatalf("#251 regression: no-discoverer branch must enter static fallback on empty stale snapshot, got %+v", instances)
	}
}

// TestGetInstances_NoDiscoverer_NoCache_HonestError 钉 #150 的文案面：真的
// 无缓存可兜底时报错，但文案只陈述已验证的事实——此前「no cached
// instances」在缓存里有（过期）实例时与事实相反，把排查方向带偏。
func TestGetInstances_NoDiscoverer_NoCache_HonestError(t *testing.T) {
	c := newTestClient(t)
	_, err := c.GetInstances(context.Background(), "svc-none")
	if err == nil {
		t.Fatal("expected error when no discoverer and no cache/fallback at all")
	}
	if strings.Contains(err.Error(), "no cached instances") {
		t.Fatalf("#150 regression: error message must not assert an unverified condition, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "svc-none") {
		t.Fatalf("error must name the service, got %q", err.Error())
	}
}
