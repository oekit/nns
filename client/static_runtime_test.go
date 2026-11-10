package client

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// TestAddStaticInstance_FlipToShortCircuitWarns 钉住 #128 的「静默」半边：
// 运行期写入口把服务从「查注册中心」切成「静态短路」时必须 Warn 点明后果
// （此前仅一行 info，运维热加备用地址静默失联、无任何告警面）。
func TestAddStaticInstance_FlipToShortCircuitWarns(t *testing.T) {
	logs := captureLogs(t)
	c := newTestClient(t, WithDiscoverer(&registrymocks.Discoverer{
		Instances: []registry.Instance{{IP: "10.0.0.2", Port: 9090}},
	}))

	c.AddStaticInstance("pay", registry.Instance{IP: "10.0.0.1", Port: 8080})

	flipWarned := false
	for _, l := range logs.snapshot() {
		if strings.HasPrefix(l, "WARN") && strings.Contains(l, "short-circuit") {
			flipWarned = true
		}
	}
	if !flipWarned {
		t.Fatalf("flipping a service to static short-circuit must Warn (got only: %v)", logs.snapshot())
	}
}

// TestAddStaticFallback_KeepsRegistryDiscovery 钉住 #128 的修复面：运行期
// 热加备用地址（AddStaticFallback）是兜底语义——注册中心发现保持不变，
// 注册中心有实例时以注册中心为准；注册中心路径失败时静态地址兜底。
func TestAddStaticFallback_KeepsRegistryDiscovery(t *testing.T) {
	static := registry.Instance{IP: "10.0.0.1", Port: 8080}

	t.Run("注册中心有实例时以注册中心为准", func(t *testing.T) {
		c := newTestClient(t, WithDiscoverer(&registrymocks.Discoverer{
			Instances: []registry.Instance{{IP: "10.0.0.2", Port: 9090}},
		}))
		c.AddStaticFallback("pay", static)

		got, err := c.GetInstances(context.Background(), "pay")
		if err != nil {
			t.Fatalf("GetInstances: %v", err)
		}
		if len(got) != 1 || got[0].IP != "10.0.0.2" {
			t.Fatalf("registry must win over hot-added backup, got %+v", got)
		}
	})

	t.Run("注册中心故障时备用地址兜底", func(t *testing.T) {
		c := newTestClient(t, WithDiscoverer(&registrymocks.Discoverer{
			Err: errors.New("nacos down"),
		}))
		c.AddStaticFallback("pay", static)

		got, err := c.GetInstances(context.Background(), "pay")
		if err != nil {
			t.Fatalf("GetInstances must fall back to hot-added backup, got: %v", err)
		}
		if len(got) != 1 || got[0].IP != "10.0.0.1" {
			t.Fatalf("backup must serve when registry is down, got %+v", got)
		}
	})
}

// TestSetStaticFallback_KeepsRegistryDiscovery 同上，覆盖覆盖式入口。
func TestSetStaticFallback_KeepsRegistryDiscovery(t *testing.T) {
	c := newTestClient(t, WithDiscoverer(&registrymocks.Discoverer{
		Instances: []registry.Instance{{IP: "10.0.0.2", Port: 9090}},
	}))
	c.SetStaticFallback("pay", []registry.Instance{{IP: "10.0.0.1", Port: 8080}})

	got, err := c.GetInstances(context.Background(), "pay")
	if err != nil {
		t.Fatalf("GetInstances: %v", err)
	}
	if len(got) != 1 || got[0].IP != "10.0.0.2" {
		t.Fatalf("registry must win over backup set via SetStaticFallback, got %+v", got)
	}
}

// TestRuntimeStaticWrites_FollowExistingFallbackSemantics 钉住「已有语义
// 标记跟随」：已配兜底语义的服务被 AddStaticInstance/SetStaticInstances
// 写入后仍是兜底，不被切成短路、也不打切路径 Warn（#128）。
func TestRuntimeStaticWrites_FollowExistingFallbackSemantics(t *testing.T) {
	logs := captureLogs(t)
	c := newTestClient(t,
		WithStaticFallback("pay", []registry.Instance{{IP: "10.0.0.1", Port: 8080}}),
		WithDiscoverer(&registrymocks.Discoverer{
			Instances: []registry.Instance{{IP: "10.0.0.2", Port: 9090}},
		}),
	)

	c.AddStaticInstance("pay", registry.Instance{IP: "10.0.0.3", Port: 7070})
	c.SetStaticInstances("pay", []registry.Instance{{IP: "10.0.0.3", Port: 7070}})

	got, err := c.GetInstances(context.Background(), "pay")
	if err != nil {
		t.Fatalf("GetInstances: %v", err)
	}
	if len(got) != 1 || got[0].IP != "10.0.0.2" {
		t.Fatalf("fallback-marked service must stay registry-first after runtime writes, got %+v", got)
	}
	for _, l := range logs.snapshot() {
		if strings.HasPrefix(l, "WARN") && strings.Contains(l, "short-circuit") {
			t.Fatalf("fallback-marked service must not flip to short-circuit, got warn: %v", l)
		}
	}
}
