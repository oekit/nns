package client

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/oekit/nns/metadata"
	"github.com/oekit/registry/registrymocks"
)

// mustBasic 构造带指定连接超时的本地服务配置（测试辅助）。
func mustBasic(t *testing.T, connTimeout int) *metadata.ServiceBasic {
	t.Helper()
	b := metadata.NewServiceBasic()
	b.ConnTimeout = connTimeout
	return b
}

// TestServiceConfig_ReturnsFetchError 钉住 #77：配置拉取失败必须把真实错误
// 上抛，而不是吞掉后返回默认超时值。
func TestServiceConfig_ReturnsFetchError(t *testing.T) {
	store := registrymocks.NewConfigStore()
	store.Err = errors.New("config center unreachable")
	c := newTestClient(t, WithConfigStore(store))

	_, err := c.ServiceConfig(context.Background(), "svc-a")
	if err == nil {
		t.Fatal("ServiceConfig must return the underlying fetch error")
	}
	if !strings.Contains(err.Error(), "config center unreachable") {
		t.Errorf("error should wrap the original fetch error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "svc-a") {
		t.Errorf("error should name the service, got: %v", err)
	}
}

// TestServiceConfig_ReturnsUnmarshalError 配置 JSON 损坏返回解析错误（#77）：
// 「配置写坏」应显式暴露，而非静默降级默认值。
func TestServiceConfig_ReturnsUnmarshalError(t *testing.T) {
	store := registrymocks.NewConfigStore()
	store.Configs["svc-b/DEFAULT_GROUP"] = `{"conn_timeout": "not-a-number"}`
	c := newTestClient(t, WithConfigStore(store))

	_, err := c.ServiceConfig(context.Background(), "svc-b")
	if err == nil {
		t.Fatal("ServiceConfig must return the unmarshal error")
	}
	if !strings.Contains(err.Error(), "unmarshal") {
		t.Errorf("error should mention unmarshal, got: %v", err)
	}
}

// TestServiceConfig_NoConfigStore 非 mock 且未注入 ConfigStore 时返回
// ErrConfigStoreUnavailable——此前该场景在门面 GetService 上表现为
// 「恒成功 + 默认超时值」（#77）。
func TestServiceConfig_NoConfigStore(t *testing.T) {
	c := newTestClient(t) // 非 mock、无 configStore

	_, err := c.ServiceConfig(context.Background(), "svc-c")
	if !errors.Is(err, ErrConfigStoreUnavailable) {
		t.Fatalf("expected ErrConfigStoreUnavailable, got: %v", err)
	}
}

// TestServiceConfig_EmptyContent_DefaultsNilError 配置未发布（content 为空）
// 不算错误：返回默认值 + nil，与 ServiceWithName 一致。
func TestServiceConfig_EmptyContent_DefaultsNilError(t *testing.T) {
	store := registrymocks.NewConfigStore()
	c := newTestClient(t, WithConfigStore(store))

	basic, err := c.ServiceConfig(context.Background(), "svc-d")
	if err != nil {
		t.Fatalf("empty content must not error, got: %v", err)
	}
	if basic.ConnTimeout != 1000 || basic.ReadTimeout != 5000 {
		t.Errorf("expected default timeouts 1000/5000, got %d/%d", basic.ConnTimeout, basic.ReadTimeout)
	}
}

// TestServiceConfig_ParsesConfiguredValues 正常路径：拉到的配置如实返回。
func TestServiceConfig_ParsesConfiguredValues(t *testing.T) {
	store := registrymocks.NewConfigStore()
	store.Configs["svc-e/DEFAULT_GROUP"] = `{"conn_timeout": 2000, "read_timeout": 8000, "retry": 5}`
	c := newTestClient(t, WithConfigStore(store))

	basic, err := c.ServiceConfig(context.Background(), "svc-e")
	if err != nil {
		t.Fatalf("ServiceConfig: %v", err)
	}
	if basic.ConnTimeout != 2000 || basic.ReadTimeout != 8000 || basic.Retry != 5 {
		t.Errorf("parsed config = %+v, want 2000/8000/5", basic)
	}
}

// TestServiceConfig_MockMode mock 模式：取本地配置，未配置则默认值，不报错。
func TestServiceConfig_MockMode(t *testing.T) {
	c := New(WithMockMode(true),
		WithLocalServiceConfig("svc-f", mustBasic(t, 3000)))

	if basic, err := c.ServiceConfig(context.Background(), "svc-f"); err != nil || basic.ConnTimeout != 3000 {
		t.Errorf("mock configured service = %+v, err %v; want conn_timeout 3000", basic, err)
	}
	if _, err := c.ServiceConfig(context.Background(), "svc-unknown"); err != nil {
		t.Errorf("mock unknown service must not error, got: %v", err)
	}
}
