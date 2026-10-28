package metadata

import "testing"

// TestGetTimeoutRetryConfig_MatchingSemantics 钉住 `GetTimeoutRetryConfig` 的
// **真实**匹配口径：只有 api 参与匹配。
//
// 缺陷成因：函数文档写「根据协议、服务名和接口名获取超时重试配置」，但规则项
// `TimeoutRetry` 只有 `Api`/`Type` 两个维度、**没有协议与服务名字段** ⇒
// protocol 与 serviceName 两个参数被静默忽略 ⇒ 照文档理解会以为「换个协议
// 查到的规则不同」，实际拿到的是同一条规则。
//
// 本用例把真实口径钉成可执行的事实：换协议/换服务名**不改变**命中结果
// （文档若改成参与匹配，本用例当场红）。
func TestGetTimeoutRetryConfig_MatchingSemantics(t *testing.T) {
	sb := &ServiceBasic{
		TimeoutRetryCfg: []*TimeoutRetry{
			{Api: "/api/get", Type: 1, Timeout: 5000, Retry: 3, Enable: true},
		},
	}

	timeout, retry, hit := sb.GetTimeoutRetryConfig("http", "svc-a", "/api/get")
	if !hit {
		t.Fatal("应命中")
	}
	if timeout != 5000 || retry != 3 {
		t.Fatalf("timeout/retry = %d/%d，期望 5000/3", timeout, retry)
	}

	// 换协议 / 换服务名：当前**不参与匹配** ⇒ 结果不变。
	for _, c := range []struct{ protocol, service string }{
		{"grpc", "svc-a"},
		{"http", "svc-b"},
		{"redis", "svc-b"},
	} {
		timeout, retry, hit := sb.GetTimeoutRetryConfig(c.protocol, c.service, "/api/get")
		if !hit || timeout != 5000 || retry != 3 {
			t.Errorf("protocol=%q service=%q → (%d,%d,%v)，期望与协议/服务名无关的 (5000,3,true)",
				c.protocol, c.service, timeout, retry, hit)
		}
	}

	// 未命中的 api：返回零值 + false。
	if _, _, hit := sb.GetTimeoutRetryConfig("http", "svc-a", "/api/unknown"); hit {
		t.Error("未知的 api 不应命中")
	}
}

// TestGetTimeoutRetryConfig_DisabledRuleSkipped Enable=false 的规则不参与匹配
// （既有口径，回归护栏）。
func TestGetTimeoutRetryConfig_DisabledRuleSkipped(t *testing.T) {
	sb := &ServiceBasic{
		TimeoutRetryCfg: []*TimeoutRetry{
			{Api: "/api/list", Timeout: 3000, Retry: 1, Enable: false},
		},
	}
	if _, _, hit := sb.GetTimeoutRetryConfig("http", "svc", "/api/list"); hit {
		t.Error("Enable=false 的规则不应命中")
	}
}
