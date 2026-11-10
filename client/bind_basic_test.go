package client

import (
	"context"
	"testing"

	"github.com/oekit/nns/metadata"
	"github.com/oekit/registry/registrymocks"
)

// TestBindServiceBasic_ExplicitSmallTimeoutsPreserved 钉住 #127：显式配置的
// 1–49ms 低延迟超时是调用方的有意取值，不得被静默改写成默认值
// （此前 <50 一律钳成 1000/5000ms，20ms 被吞 50 倍且零日志）。
func TestBindServiceBasic_ExplicitSmallTimeoutsPreserved(t *testing.T) {
	c := newTestClient(t)
	b := &metadata.ServiceBasic{ConnectionConfig: metadata.ConnectionConfig{
		ConnTimeout:  20,
		ReadTimeout:  30,
		WriteTimeout: 40,
		Retry:        5,
	}}
	c.BindServiceBasic("svc", b)
	if b.ConnTimeout != 20 || b.ReadTimeout != 30 || b.WriteTimeout != 40 {
		t.Fatalf("explicit small timeouts must be preserved, got %d/%d/%d, want 20/30/40",
			b.ConnTimeout, b.ReadTimeout, b.WriteTimeout)
	}
	if b.Retry != 5 {
		t.Fatalf("Retry = %d, want 5", b.Retry)
	}
}

// TestBindServiceBasic_ZeroValuesBackfilled 未配置（<=0）仍回填默认值。
func TestBindServiceBasic_ZeroValuesBackfilled(t *testing.T) {
	c := newTestClient(t)
	b := &metadata.ServiceBasic{}
	c.BindServiceBasic("svc", b)
	if b.ConnTimeout != 1000 || b.ReadTimeout != 5000 || b.WriteTimeout != 5000 {
		t.Fatalf("unconfigured timeouts must backfill defaults, got %d/%d/%d",
			b.ConnTimeout, b.ReadTimeout, b.WriteTimeout)
	}
}

// TestBindServiceBasic_RetryAboveLimitKept 钉住 #127：Retry>15 不再静默钳 15
// （显式值原样保留，防呆只 Warn）。
func TestBindServiceBasic_RetryAboveLimitKept(t *testing.T) {
	c := newTestClient(t)
	b := &metadata.ServiceBasic{ConnectionConfig: metadata.ConnectionConfig{Retry: 20}}
	c.BindServiceBasic("svc", b)
	if b.Retry != 20 {
		t.Fatalf("explicit Retry = %d must be preserved, want 20", b.Retry)
	}
}

// TestBindServiceBasic_NegativeRetryClampedToZero 负重试无意义，钳 0
// （唯一保留的改写，且必须留日志——日志面由 Warn 保障）。
func TestBindServiceBasic_NegativeRetryClampedToZero(t *testing.T) {
	c := newTestClient(t)
	b := &metadata.ServiceBasic{ConnectionConfig: metadata.ConnectionConfig{Retry: -1}}
	c.BindServiceBasic("svc", b)
	if b.Retry != 0 {
		t.Fatalf("negative Retry must clamp to 0, got %d", b.Retry)
	}
}

// TestServiceConfig_LowLatencyConfigPreserved 端到端（issue 探针形态）：
// 配置中心原文 conn_timeout:20 / read_timeout:30 / retry:20 与生效值一致。
func TestServiceConfig_LowLatencyConfigPreserved(t *testing.T) {
	store := registrymocks.NewConfigStore()
	store.Configs["svc-lat/DEFAULT_GROUP"] = `{"conn_timeout": 20, "read_timeout": 30, "write_timeout": 40, "retry": 20}`
	c := newTestClient(t, WithConfigStore(store))

	basic, err := c.ServiceConfig(context.Background(), "svc-lat")
	if err != nil {
		t.Fatalf("ServiceConfig: %v", err)
	}
	if basic.ConnTimeout != 20 || basic.ReadTimeout != 30 || basic.WriteTimeout != 40 || basic.Retry != 20 {
		t.Fatalf("config-center values must survive BindServiceBasic, got %d/%d/%d/%d, want 20/30/40/20",
			basic.ConnTimeout, basic.ReadTimeout, basic.WriteTimeout, basic.Retry)
	}
}
