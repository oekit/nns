package nns

import (
	"context"
	"errors"
	"testing"

	"github.com/oekit/nns/client"
	nacospkg "github.com/oekit/registry/nacos"
	"github.com/oekit/registry/registrymocks"
)

// TestGetService_ReturnsRealError 钉住 #77：门面 GetService 此前恒返回
// (&basic, nil)，配置拉取失败被吞成默认超时值；现在必须把真实错误上抛。
func TestGetService_ReturnsRealError(t *testing.T) {
	store := registrymocks.NewConfigStore()
	store.Err = errors.New("config center unreachable")
	nnsc := &NNSClient{client: client.New(client.WithConfigStore(store))}

	if _, err := nnsc.GetService(context.Background(), "svc-a"); err == nil {
		t.Fatal("GetService must return the underlying fetch error (#77)")
	}
}

// TestGetService_NoProviderErrors 非 mock 且未注入 provider 时返回
// ErrConfigStoreUnavailable，不再伪装成功。
func TestGetService_NoProviderErrors(t *testing.T) {
	nnsc := &NNSClient{client: client.New()}

	_, err := nnsc.GetService(context.Background(), "svc-a")
	if !errors.Is(err, client.ErrConfigStoreUnavailable) {
		t.Fatalf("expected client.ErrConfigStoreUnavailable, got: %v", err)
	}
}

// TestGetService_MockMode_ReturnsDefaults mock 模式下仍是软语义：默认值 + nil。
func TestGetService_MockMode_ReturnsDefaults(t *testing.T) {
	nnsc := New(nacospkg.Bootstrap{}, WithMockMode(true))

	basic, err := nnsc.GetService(context.Background(), "svc-any")
	if err != nil {
		t.Fatalf("mock mode GetService must not error, got: %v", err)
	}
	if basic == nil || basic.ConnTimeout != 1000 {
		t.Errorf("mock mode should return defaults, got %+v", basic)
	}
}

// TestGetService_ParsesConfiguredValues 正常路径：配置如实解析返回。
func TestGetService_ParsesConfiguredValues(t *testing.T) {
	store := registrymocks.NewConfigStore()
	store.Configs["svc-b/DEFAULT_GROUP"] = `{"conn_timeout": 2500}`
	nnsc := &NNSClient{client: client.New(
		client.WithConfigStore(store), client.WithConfigGroup("DEFAULT_GROUP"))}

	basic, err := nnsc.GetService(context.Background(), "svc-b")
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	if basic.ConnTimeout != 2500 {
		t.Errorf("ConnTimeout = %d, want 2500", basic.ConnTimeout)
	}
}
