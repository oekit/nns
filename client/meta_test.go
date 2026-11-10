package client

import (
	"testing"

	"github.com/oekit/nns/metadata"
)

// TestBindServiceBasic_ExplicitSub50msTimeoutKept 回归（nns#127）：显式
// 低延迟配置（1–49ms）必须原样生效——旧实现把「<50」当「未配置」，显式
// 20ms 被静默吞成 1000ms（放大 50 倍、零日志），后端卡死时调用方挂 5s
// 而非 30ms，配置中心原文与生效值对不上。修复后只对 <=0 回填默认。
func TestBindServiceBasic_ExplicitSub50msTimeoutKept(t *testing.T) {
	c := newTestClient(t)
	basic := &metadata.ServiceBasic{
		ConnectionConfig: metadata.ConnectionConfig{
			ConnTimeout:  20,
			ReadTimeout:  30,
			WriteTimeout: 40,
		},
	}

	c.BindServiceBasic("svc-lowlatency", basic)

	if basic.ConnTimeout != 20 || basic.ReadTimeout != 30 || basic.WriteTimeout != 40 {
		t.Fatalf("explicit sub-50ms timeouts must be kept as configured (nns#127), got conn=%d read=%d write=%d",
			basic.ConnTimeout, basic.ReadTimeout, basic.WriteTimeout)
	}
}

// TestBindServiceBasic_ExplicitRetryAboveCapKept 显式 Retry>15 保留原值：
// 「上限」是防呆建议，不是静默改写治理意图的依据（nns#127）。
func TestBindServiceBasic_ExplicitRetryAboveCapKept(t *testing.T) {
	c := newTestClient(t)
	basic := &metadata.ServiceBasic{ConnectionConfig: metadata.ConnectionConfig{Retry: 20}}

	c.BindServiceBasic("svc", basic)

	if basic.Retry != 20 {
		t.Fatalf("explicit retry above advisory cap must be kept (nns#127), got %d", basic.Retry)
	}
}

// TestBindServiceBasic_NegativeRetryBackfilled 负值 Retry 视为未配置，
// 回填 0（不重试）——负值不是「重试很多次」的治理意图。
func TestBindServiceBasic_NegativeRetryBackfilled(t *testing.T) {
	c := newTestClient(t)
	basic := &metadata.ServiceBasic{ConnectionConfig: metadata.ConnectionConfig{Retry: -1}}

	c.BindServiceBasic("svc", basic)

	if basic.Retry != 0 {
		t.Fatalf("negative retry must backfill to 0, got %d", basic.Retry)
	}
}

// TestBindServiceBasic_UnconfiguredBackfilled 未配置（<=0）仍回填默认值
// ——本函数「设置默认值」的既有契约，修复后继续成立（钉住防回归）。
func TestBindServiceBasic_UnconfiguredBackfilled(t *testing.T) {
	c := newTestClient(t)
	basic := &metadata.ServiceBasic{}

	c.BindServiceBasic("svc", basic)

	if basic.ConnTimeout != 1000 || basic.ReadTimeout != 5000 || basic.WriteTimeout != 5000 {
		t.Fatalf("unconfigured timeouts must backfill defaults 1000/5000/5000, got %d/%d/%d",
			basic.ConnTimeout, basic.ReadTimeout, basic.WriteTimeout)
	}
	if basic.Retry != 0 {
		t.Errorf("unconfigured retry stays 0 (NewServiceBasic 的 Retry=3 由配置默认值提供), got %d", basic.Retry)
	}
}

// TestBindServiceBasic_ExplicitValuesPassThrough 服务治理配置的正常路径：
// 显式正值（≥50ms / 1–15 次）如实生效（钉住既有用例同口径）。
func TestBindServiceBasic_ExplicitValuesPassThrough(t *testing.T) {
	c := newTestClient(t)
	basic := &metadata.ServiceBasic{
		ConnectionConfig: metadata.ConnectionConfig{
			ConnTimeout:  2000,
			ReadTimeout:  8000,
			WriteTimeout: 9000,
			Retry:        5,
		},
	}

	c.BindServiceBasic("com.example.shop.order", basic)

	if basic.ConnTimeout != 2000 || basic.ReadTimeout != 8000 || basic.WriteTimeout != 9000 || basic.Retry != 5 {
		t.Fatalf("explicit values must pass through, got %+v", basic)
	}
}
