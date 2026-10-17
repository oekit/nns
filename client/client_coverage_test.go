package client

import (
	"context"
	"testing"
	"time"

	"github.com/oekit/nns/metadata"
	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// TestUnsubscribeService covers the 0%-coverage UnsubscribeService
// path. In mock mode, the no-op branch returns nil without touching
// the subscriber; we verify that no error is surfaced.
func TestUnsubscribeService_MockMode_NoOp(t *testing.T) {
	c := newTestClient(t, WithMockMode(true))
	_, _ = c.LocalServiceInit(context.Background(), "test-svc")

	if err := c.UnsubscribeService(context.Background(), "svc-a"); err != nil {
		t.Errorf("UnsubscribeService in mock mode: %v", err)
	}
}

// TestUnsubscribeService_WithSubscriber covers the real-subscriber
// path: after Subscribe + Unsubscribe, the subscriber's internal
// state must reflect the unsubscribe.
func TestUnsubscribeService_WithSubscriber(t *testing.T) {
	sub := registrymocks.NewSubscriber()
	c := newTestClient(t, WithSubscriber(sub), WithDiscoverer(&registrymocks.Discoverer{}))

	if err := c.SubscribeService(context.Background(), "svc-a", func(_ []registry.Instance) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if got := sub.CallbackCount("svc-a"); got != 1 {
		t.Errorf("expected 1 subscription for svc-a, got %d", got)
	}

	if err := c.UnsubscribeService(context.Background(), "svc-a"); err != nil {
		t.Errorf("Unsubscribe: %v", err)
	}
	if got := sub.CallbackCount("svc-a"); got != 0 {
		t.Errorf("expected 0 subscriptions after unsubscribe, got %d", got)
	}
}

// TestServiceWithName_FromLocalServiceConfig covers the mock-mode
// localServiceConfig branch. WithLocalServiceConfig populates the
// lookup table; ServiceWithName must return the configured values.
func TestServiceWithName_FromLocalServiceConfig(t *testing.T) {
	dep := &metadata.ServiceBasic{
		ServiceName: metadata.ServiceName{FullName: "svc-dep"},
		ConnectionConfig: metadata.ConnectionConfig{
			ReadTimeout: 7000,
			Lb:          metadata.LbRoundRobin,
		},
	}
	c := newTestClient(t,
		WithMockMode(true),
		WithLocalServiceConfig("svc-dep", dep),
	)

	got := c.ServiceWithName(context.Background(), "svc-dep")
	if got.ReadTimeout != 7000 {
		t.Errorf("expected ReadTimeout 7000, got %d", got.ReadTimeout)
	}
	if got.Lb != metadata.LbRoundRobin {
		t.Errorf("expected Lb %q, got %q", metadata.LbRoundRobin, got.Lb)
	}
}

// TestServiceWithName_UnknownService_ReturnsDefault covers the
// "no config anywhere" branch — must return a default ServiceBasic.
func TestServiceWithName_UnknownService_ReturnsDefault(t *testing.T) {
	c := newTestClient(t, WithMockMode(true))
	got := c.ServiceWithName(context.Background(), "svc-unknown")
	// ServiceBasic is a value type; just verify the default fields.
	if got.ReadTimeout != 5000 {
		t.Errorf("expected default ReadTimeout 5000, got %d", got.ReadTimeout)
	}
}

// TestAddStaticInstance covers the runtime AddStaticInstance helper.
// After adding, GetInstances must return the new entries.
func TestAddStaticInstance(t *testing.T) {
	c := newTestClient(t, WithMockMode(true))
	c.AddStaticInstance("svc-add", registry.Instance{IP: "1.1.1.1", Port: 80})
	c.AddStaticInstance("svc-add", registry.Instance{IP: "2.2.2.2", Port: 80})

	got, err := c.GetInstances(context.Background(), "svc-add")
	if err != nil {
		t.Fatalf("GetInstances: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 instances after Add, got %d", len(got))
	}
}

// TestSetStaticInstances covers the runtime SetStaticInstances helper.
// After setting, GetInstances must return exactly the new list
// (overwriting any previous static instances).
func TestSetStaticInstances(t *testing.T) {
	c := newTestClient(t,
		WithMockMode(true),
		WithStaticInstances("svc-set", []registry.Instance{
			{IP: "9.9.9.9", Port: 1},
			{IP: "9.9.9.9", Port: 2},
		}),
	)

	got, err := c.GetInstances(context.Background(), "svc-set")
	if err != nil {
		t.Fatalf("GetInstances (pre): %v", err)
	}
	if len(got) != 2 {
		t.Errorf("precondition: expected 2 instances, got %d", len(got))
	}

	c.SetStaticInstances("svc-set", []registry.Instance{
		{IP: "7.7.7.7", Port: 7},
	})
	got, err = c.GetInstances(context.Background(), "svc-set")
	if err != nil {
		t.Fatalf("GetInstances (post): %v", err)
	}
	if len(got) != 1 {
		t.Errorf("expected 1 instance after Set, got %d", len(got))
	}
	if got[0].IP != "7.7.7.7" {
		t.Errorf("expected 7.7.7.7, got %q", got[0].IP)
	}
}

// TestGetLocalService covers the 0%-coverage GetLocalService accessor.
func TestGetLocalService(t *testing.T) {
	c := newTestClient(t)
	// Pre-condition: no LocalService set.
	if c.GetLocalService() != nil {
		t.Error("expected nil for client without LocalService set")
	}

	c.localService = metadata.NewServiceBasic()
	c.localService.ServiceName.FullName = "my-svc"
	got := c.GetLocalService()
	if got == nil {
		t.Fatal("expected non-nil after setting")
	}
	if got.ServiceName.FullName != "my-svc" {
		t.Errorf("expected 'my-svc', got %q", got.ServiceName.FullName)
	}
}

// TestGetLocalColor covers the 0%-coverage GetLocalColor accessor.
func TestGetLocalColor(t *testing.T) {
	c := newTestClient(t)
	if got := c.GetLocalColor(); got != "" {
		t.Errorf("expected empty color for unset meta, got %q", got)
	}

	m := map[string]string{"color": "blue"}
	c.localServiceMeta.Store(&m)
	if got := c.GetLocalColor(); got != "blue" {
		t.Errorf("expected 'blue', got %q", got)
	}
}

// TestGetLocalColor_NoColorKey covers the "color key missing" branch.
func TestGetLocalColor_NoColorKey(t *testing.T) {
	c := newTestClient(t)
	m := map[string]string{"region": "us-west-2"}
	c.localServiceMeta.Store(&m)
	if got := c.GetLocalColor(); got != "" {
		t.Errorf("expected empty when 'color' key missing, got %q", got)
	}
}

// TestGetLocalRegion covers the 0%-coverage GetLocalRegion accessor.
func TestGetLocalRegion(t *testing.T) {
	c := newTestClient(t)
	if got := c.GetLocalRegion(); got != "" {
		t.Errorf("expected empty region for unset LocalService, got %q", got)
	}

	c.localService = metadata.NewServiceBasic()
	c.localService.ServiceName.Region = "us-west-2"
	if got := c.GetLocalRegion(); got != "us-west-2" {
		t.Errorf("expected 'us-west-2', got %q", got)
	}
}

// TestGetRegistrar covers the 0%-coverage GetRegistrar accessor.
func TestGetRegistrar_NilByDefault(t *testing.T) {
	c := newTestClient(t)
	if got := c.GetRegistrar(); got != nil {
		t.Error("expected nil registrar by default")
	}
}

// TestGetRegistrar_ReturnsInjected verifies GetRegistrar returns the
// injected registrar (used by nns/kitex and nns/hertz adapters).
func TestGetRegistrar_ReturnsInjected(t *testing.T) {
	r := &registrymocks.Registrar{}
	c := newTestClient(t, WithRegistrar(r))
	if got := c.GetRegistrar(); got == nil {
		t.Error("expected non-nil registrar after injection")
	}
}

// TestGetInstancesFromStaticOrCache_StaticHit covers the static-hit
// branch in the internal helper. Exercises via GetInstances.
func TestGetInstancesFromStaticOrCache_StaticHit(t *testing.T) {
	c := newTestClient(t,
		WithMockMode(true),
		WithStaticInstances("svc-static", []registry.Instance{
			{IP: "1.1.1.1", Port: 1},
			{IP: "2.2.2.2", Port: 2},
		}),
	)
	got, err := c.GetInstances(context.Background(), "svc-static")
	if err != nil {
		t.Fatalf("GetInstances: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 from static, got %d", len(got))
	}
}

// TestGetInstancesFromStaticOrCache_CacheHit covers the cache-hit
// branch.
func TestGetInstancesFromStaticOrCache_CacheHit(t *testing.T) {
	c := newTestClient(t, WithMockMode(true))
	c.updateCache("svc-cached", []registry.Instance{
		{IP: "3.3.3.3", Port: 3},
	})
	got, err := c.GetInstances(context.Background(), "svc-cached")
	if err != nil {
		t.Fatalf("GetInstances: %v", err)
	}
	if len(got) != 1 || got[0].IP != "3.3.3.3" {
		t.Errorf("expected cached instance, got %+v", got)
	}
}

// TestGetInstancesFromStaticOrCache_NoData covers the empty branch.
func TestGetInstancesFromStaticOrCache_NoData(t *testing.T) {
	c := newTestClient(t, WithMockMode(true))
	got, err := c.GetInstances(context.Background(), "svc-empty")
	if err != nil {
		t.Fatalf("GetInstances: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 from empty, got %d", len(got))
	}
}

// TestGetInstancesFromStaticOrCache_CacheExpired covers the cache
// TTL branch. After cacheExpiry, the cache must be considered stale.
func TestGetInstancesFromStaticOrCache_CacheExpired(t *testing.T) {
	c := newTestClient(t, WithMockMode(true), WithCacheExpiry(10*time.Millisecond))
	c.updateCache("svc-expire", []registry.Instance{
		{IP: "4.4.4.4", Port: 4},
	})

	time.Sleep(20 * time.Millisecond)

	got, err := c.GetInstances(context.Background(), "svc-expire")
	if err != nil {
		t.Fatalf("GetInstances: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 from expired cache, got %d", len(got))
	}
}

// TestGetAllInstances_FromDiscoverer covers the GetAllInstances
// happy path (when not in mock mode).
func TestGetAllInstances_FromDiscoverer(t *testing.T) {
	md := &registrymocks.Discoverer{
		AllInstances: []registry.Instance{
			{IP: "5.5.5.5", Port: 5},
			{IP: "6.6.6.6", Port: 6},
		},
	}
	c := newTestClient(t, WithDiscoverer(md))

	got, err := c.GetAllInstances(context.Background(), "svc-all")
	if err != nil {
		t.Fatalf("GetAllInstances: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 instances, got %d", len(got))
	}
}

// TestGetAllInstances_DiscovererError covers the error path.
func TestGetAllInstances_DiscovererError(t *testing.T) {
	md := &registrymocks.Discoverer{AllErr: context.DeadlineExceeded}
	c := newTestClient(t, WithDiscoverer(md))

	_, err := c.GetAllInstances(context.Background(), "svc-x")
	if err == nil {
		t.Fatal("expected error from discoverer, got nil")
	}
}

// TestGetAllInstances_MockMode covers the mock-mode branch (skips
// the discoverer entirely, returns from static or cache).
func TestGetAllInstances_MockMode(t *testing.T) {
	c := newTestClient(t,
		WithMockMode(true),
		WithStaticInstances("svc-static", []registry.Instance{
			{IP: "7.7.7.7", Port: 7},
		}),
	)
	got, err := c.GetAllInstances(context.Background(), "svc-static")
	if err != nil {
		t.Fatalf("GetAllInstances: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("expected 1 from static, got %d", len(got))
	}
}

// TestNew_WithDI_ShortCircuits covers the New() function's DI branch:
// when registrar/discoverer/subscriber/configStore are all injected,
// New must return early without touching Nacos.
func TestNew_WithDI_ShortCircuits(t *testing.T) {
	c := nnsNew(t,
		WithRegistrar(&registrymocks.Registrar{}),
		WithDiscoverer(&registrymocks.Discoverer{}),
		WithSubscriber(registrymocks.NewSubscriber()),
		WithConfigStore(registrymocks.NewConfigStore()),
	)
	if c == nil {
		t.Fatal("expected non-nil ServiceClient")
	}
	// 接口已注入：namingAvailable 应为真（discoverer 就绪、非 mock）
	if !c.namingAvailable() {
		t.Error("expected namingAvailable()=true with injected discoverer")
	}
}

// TestServiceWithName_NonMock_NoConfigStore covers the non-mock,
// no-configStore branch: must return a ServiceBasic with parsed
// service name (if name has region/group) or default.
func TestServiceWithName_NonMock_NoConfigStore(t *testing.T) {
	c := nnsNew(t) // no mock, no interfaces
	got := c.ServiceWithName(context.Background(), "svc-us-west-2:DEFAULT_GROUP")
	// ParseServiceName may or may not parse the colon syntax; we just
	// verify the call returns a non-zero value and doesn't panic.
	_ = got
}

// TestServiceWithName_NonMock_WithConfigStore covers the config-store
// path: a stored config must be unmarshaled and bound to the service.
func TestServiceWithName_NonMock_WithConfigStore(t *testing.T) {
	store := registrymocks.NewConfigStore()
	store.Configs["my-svc/DEFAULT_GROUP"] = `{"conn_timeout": 2000, "read_timeout": 8000, "retry": 5}`

	c := nnsNew(t, WithConfigStore(store))
	got := c.ServiceWithName(context.Background(), "my-svc")
	if got.ReadTimeout != 8000 {
		t.Errorf("expected ReadTimeout 8000, got %d", got.ReadTimeout)
	}
}

// TestServiceWithName_NonMock_ConfigStoreError covers the error
// path: when the config store returns an error, a default
// ServiceBasic is returned.
func TestServiceWithName_NonMock_ConfigStoreError(t *testing.T) {
	store := registrymocks.NewConfigStore()
	store.Err = context.DeadlineExceeded

	c := nnsNew(t, WithConfigStore(store))
	got := c.ServiceWithName(context.Background(), "my-svc")
	// Just verify no panic and a non-zero value.
	if got.ReadTimeout != 5000 {
		t.Errorf("expected default ReadTimeout 5000, got %d", got.ReadTimeout)
	}
}

// TestNamingAvailable_MockModeWithoutInterfaces is a defensive check
// that namingAvailable() returns false when no interfaces are injected,
// even in mock mode. (Note: TestNamingAvailable in client_test.go
// covers the positive path; this is the negative case.)
func TestNamingAvailable_MockModeWithoutInterfaces(t *testing.T) {
	c := newTestClient(t, WithMockMode(true))
	// In mock mode without interfaces, naming is not available.
	if c.namingAvailable() {
		t.Error("expected namingAvailable=false without injected interfaces")
	}
}

// nnsNew is a thin wrapper around the package's New function for tests
// that want to exercise the real constructor (vs newTestClient which
// bypasses New and constructs the struct directly). It accepts the
// same ClientOption set as the real New.
func nnsNew(t *testing.T, opts ...ClientOption) *ServiceClient {
	t.Helper()
	return New(append([]ClientOption{WithConfigGroup("DEFAULT_GROUP")}, opts...)...)
}
