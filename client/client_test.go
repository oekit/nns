package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/nns/metadata"
	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// waitListening 等待 WatchService 的后台 goroutine 完成监听注册。
func waitListening(t *testing.T, m *registrymocks.ConfigStore, dataID, group string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.ListeningCountFor(dataID, group) > 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for ListenConfig registration of %s", dataID)
}

// --- Helpers ---

func newTestClient(t *testing.T, opts ...ClientOption) *ServiceClient {
	t.Helper()

	// Create a fresh ServiceClient with mock interfaces
	c := &ServiceClient{
		configGroup:        "DEFAULT_GROUP",
		selfServiceName:    "test-service",
		depService:         make(map[string]*metadata.ServiceBasic),
		instanceCallbacks:  make(map[string][]*instanceCallback),
		instanceCache:      make(map[string][]registry.Instance),
		cacheLastUpdate:    make(map[string]time.Time),
		cacheExpiry:        30 * time.Second,
		staticInstances:    make(map[string][]registry.Instance),
		localServiceConfig: make(map[string]*metadata.ServiceBasic),
	}
	for _, opt := range opts {
		opt(c)
	}
	initMeta := make(map[string]string)
	c.localServiceMeta.Store(&initMeta)
	return c
}

// --- Tests ---

func TestGetInstances_StaticInstances(t *testing.T) {
	c := newTestClient(t,
		WithStaticInstances("svc-a", []registry.Instance{
			{IP: "10.0.0.1", Port: 8080},
		}),
	)

	instances, err := c.GetInstances(context.Background(), "svc-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(instances))
	}
	if instances[0].IP != "10.0.0.1" {
		t.Errorf("expected IP 10.0.0.1, got %s", instances[0].IP)
	}
}

func TestGetInstances_FromDiscoverer(t *testing.T) {
	disc := &registrymocks.Discoverer{
		Instances: []registry.Instance{
			{Service: "svc-b", IP: "10.0.0.2", Port: 9090, Weight: 10},
		},
	}
	c := newTestClient(t, WithDiscoverer(disc))

	instances, err := c.GetInstances(context.Background(), "svc-b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(instances))
	}
	if instances[0].IP != "10.0.0.2" {
		t.Errorf("expected IP 10.0.0.2, got %s", instances[0].IP)
	}
	if instances[0].Port != 9090 {
		t.Errorf("expected Port 9090, got %d", instances[0].Port)
	}
}

func TestGetInstances_DiscoverError_FallbackToCache(t *testing.T) {
	disc := &registrymocks.Discoverer{
		Err: errors.New("nacos down"),
	}
	c := newTestClient(t, WithDiscoverer(disc))

	// Pre-populate cache
	c.updateCache("svc-c", []registry.Instance{
		{IP: "10.0.0.3", Port: 7070},
	})

	instances, err := c.GetInstances(context.Background(), "svc-c")
	if err != nil {
		t.Fatalf("should fallback to cache, got error: %v", err)
	}
	if len(instances) != 1 {
		t.Fatalf("expected 1 cached instance, got %d", len(instances))
	}
	if instances[0].IP != "10.0.0.3" {
		t.Errorf("expected cached IP 10.0.0.3, got %s", instances[0].IP)
	}
}

func TestGetInstances_DiscoverError_NoCache_Error(t *testing.T) {
	disc := &registrymocks.Discoverer{
		Err: errors.New("nacos down"),
	}
	c := newTestClient(t, WithDiscoverer(disc))

	_, err := c.GetInstances(context.Background(), "svc-d")
	if err == nil {
		t.Fatal("expected error when discover fails and no cache")
	}
}

func TestGetInstances_NoDiscoverer_FallbackToCache(t *testing.T) {
	c := newTestClient(t)
	// No discoverer set, cache has data
	c.updateCache("svc-e", []registry.Instance{
		{IP: "10.0.0.5", Port: 5050},
	})

	instances, err := c.GetInstances(context.Background(), "svc-e")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 1 {
		t.Fatalf("expected 1 cached instance, got %d", len(instances))
	}
}

func TestGetAllInstances(t *testing.T) {
	disc := &registrymocks.Discoverer{
		AllInstances: []registry.Instance{
			{IP: "10.0.0.10", Port: 8080, Weight: 5},
			{IP: "10.0.0.11", Port: 8080, Weight: 5},
		},
	}
	c := newTestClient(t, WithDiscoverer(disc))

	instances, err := c.GetAllInstances(context.Background(), "svc-f")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(instances))
	}
}

func TestSubscribeService_CallbackConvertsTypes(t *testing.T) {
	sub := registrymocks.NewSubscriber()
	c := newTestClient(t, WithSubscriber(sub))

	var received []registry.Instance
	err := c.SubscribeService(context.Background(), "svc-g", func(instances []registry.Instance) {
		received = instances
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate Nacos push
	sub.Push("svc-g", []registry.Instance{
		{Service: "svc-g", IP: "10.0.0.7", Port: 7070, Weight: 10},
	})

	if len(received) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(received))
	}
	if received[0].IP != "10.0.0.7" {
		t.Errorf("expected IP 10.0.0.7, got %s", received[0].IP)
	}
}

func TestSubscribeService_UpdatesCache(t *testing.T) {
	sub := registrymocks.NewSubscriber()
	c := newTestClient(t, WithSubscriber(sub))

	_ = c.SubscribeService(context.Background(), "svc-h", func([]registry.Instance) {})

	sub.Push("svc-h", []registry.Instance{
		{Service: "svc-h", IP: "10.0.0.8", Port: 8080},
	})

	cached := c.getInstancesFromCache("svc-h")
	if len(cached) != 1 {
		t.Fatalf("expected 1 cached instance, got %d", len(cached))
	}
}

func TestRegister(t *testing.T) {
	reg := &registrymocks.Registrar{}
	c := newTestClient(t, WithRegistrar(reg))

	err := c.Register(context.Background(), 8080, registry.WithMetadata(map[string]string{"version": "v1"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(reg.Registered) != 1 {
		t.Fatalf("expected 1 registered instance, got %d", len(reg.Registered))
	}
	inst := reg.Registered[0]
	if inst.Port != 8080 {
		t.Errorf("expected port 8080, got %d", inst.Port)
	}
	if inst.Service != "test-service" {
		t.Errorf("expected service test-service, got %s", inst.Service)
	}
	if inst.Metadata["version"] != "v1" {
		t.Errorf("expected metadata version=v1, got %v", inst.Metadata)
	}
}

func TestRegister_NoRegistrar_Error(t *testing.T) {
	c := newTestClient(t)
	err := c.Register(context.Background(), 8080)
	if err == nil {
		t.Fatal("expected error when registrar is nil")
	}
}

func TestDeregister(t *testing.T) {
	reg := &registrymocks.Registrar{}
	c := newTestClient(t, WithRegistrar(reg))

	err := c.Deregister(context.Background(), 8080)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeregister_NoRegistrar_NoError(t *testing.T) {
	c := newTestClient(t)
	err := c.Deregister(context.Background(), 8080)
	if err != nil {
		t.Fatalf("expected nil error when registrar is nil, got: %v", err)
	}
}

func TestLocalServiceInit_FromConfigStore(t *testing.T) {
	store := registrymocks.NewConfigStore()
	store.Configs["my-svc/DEFAULT_GROUP"] = `{"conn_timeout": 2000, "read_timeout": 8000, "retry": 5}`
	c := newTestClient(t, WithConfigStore(store))

	basic, err := c.LocalServiceInit(context.Background(), "my-svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if basic.ConnTimeout != 2000 {
		t.Errorf("expected ConnTimeout 2000, got %d", basic.ConnTimeout)
	}
	if basic.ReadTimeout != 8000 {
		t.Errorf("expected ReadTimeout 8000, got %d", basic.ReadTimeout)
	}
	if basic.Retry != 5 {
		t.Errorf("expected Retry 5, got %d", basic.Retry)
	}
}

func TestLocalServiceInit_ConfigStoreError_Defaults(t *testing.T) {
	store := registrymocks.NewConfigStore()
	store.Err = errors.New("nacos down")
	c := newTestClient(t, WithConfigStore(store))

	basic, err := c.LocalServiceInit(context.Background(), "svc-x")
	if err != nil {
		t.Fatalf("should fall back to defaults, got error: %v", err)
	}
	// Should return defaults
	if basic.ConnTimeout != 1000 {
		t.Errorf("expected default ConnTimeout 1000, got %d", basic.ConnTimeout)
	}
}

func TestLocalServiceInit_MockMode(t *testing.T) {
	c := newTestClient(t,
		WithMockMode(true),
		WithLocalServiceConfig("my-svc", &metadata.ServiceBasic{
			ConnectionConfig: metadata.ConnectionConfig{
				ConnTimeout: 3000,
				ReadTimeout: 9000,
				Retry:       7,
			},
		}),
	)

	basic, err := c.LocalServiceInit(context.Background(), "my-svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if basic.ConnTimeout != 3000 {
		t.Errorf("expected ConnTimeout 3000, got %d", basic.ConnTimeout)
	}
}

func TestCacheExpiry(t *testing.T) {
	c := newTestClient(t, WithCacheExpiry(100*time.Millisecond))
	c.updateCache("svc-exp", []registry.Instance{
		{IP: "10.0.0.99", Port: 9999},
	})

	// Cache should be valid immediately
	cached := c.getInstancesFromCache("svc-exp")
	if len(cached) != 1 {
		t.Fatalf("expected 1 cached instance, got %d", len(cached))
	}

	// Wait for expiry
	time.Sleep(150 * time.Millisecond)

	cached = c.getInstancesFromCache("svc-exp")
	if cached != nil {
		t.Errorf("expected nil after expiry, got %d items", len(cached))
	}
}

func TestNamingAvailable(t *testing.T) {
	c := newTestClient(t)
	if c.namingAvailable() {
		t.Error("expected false without discoverer")
	}

	c2 := newTestClient(t, WithDiscoverer(&registrymocks.Discoverer{}))
	if !c2.namingAvailable() {
		t.Error("expected true with discoverer and non-mock mode")
	}

	c3 := newTestClient(t,
		WithMockMode(true),
		WithDiscoverer(&registrymocks.Discoverer{}),
	)
	if c3.namingAvailable() {
		t.Error("expected false in mock mode")
	}
}

// countingDiscoverer 统计 Discover 调用次数的 mock。
type countingDiscoverer struct {
	mu      sync.Mutex
	calls   int
	delay   time.Duration
	result  []registry.Instance
	err     error
	allInst []registry.Instance
}

func (d *countingDiscoverer) Discover(_ context.Context, _ string) ([]registry.Instance, error) {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	if d.delay > 0 {
		time.Sleep(d.delay)
	}
	return d.result, d.err
}

func (d *countingDiscoverer) DiscoverAll(_ context.Context, _ string) ([]registry.Instance, error) {
	if d.allInst != nil {
		return d.allInst, nil
	}
	return d.result, d.err
}

func (d *countingDiscoverer) getCalls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

// TestGetInstances_FreshCacheSkipsDiscover 验证缓存未过期时不会调用 Discover。
func TestGetInstances_FreshCacheSkipsDiscover(t *testing.T) {
	disc := &countingDiscoverer{
		result: []registry.Instance{
			{IP: "10.0.0.1", Port: 8080},
		},
	}
	c := newTestClient(t, WithDiscoverer(disc))

	// 预填充新鲜缓存
	c.updateCache("svc-fresh", []registry.Instance{
		{IP: "10.0.0.99", Port: 9999},
	})

	instances, err := c.GetInstances(context.Background(), "svc-fresh")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 1 || instances[0].IP != "10.0.0.99" {
		t.Fatalf("expected cached instance 10.0.0.99, got %+v", instances)
	}
	if calls := disc.getCalls(); calls != 0 {
		t.Errorf("expected 0 Discover calls (cache fresh), got %d", calls)
	}
}

// TestGetInstances_StaleCacheTriggersDiscover 验证缓存过期后触发 Discover。
func TestGetInstances_StaleCacheTriggersDiscover(t *testing.T) {
	disc := &countingDiscoverer{
		result: []registry.Instance{
			{IP: "10.0.0.2", Port: 8080},
		},
	}
	c := newTestClient(t, WithDiscoverer(disc), WithCacheExpiry(50*time.Millisecond))

	// 预填充缓存
	c.updateCache("svc-stale", []registry.Instance{
		{IP: "10.0.0.99", Port: 9999},
	})

	// 等待缓存过期
	time.Sleep(80 * time.Millisecond)

	instances, err := c.GetInstances(context.Background(), "svc-stale")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if instances[0].IP != "10.0.0.2" {
		t.Fatalf("expected fresh instance 10.0.0.2 from discover, got %s", instances[0].IP)
	}
	if calls := disc.getCalls(); calls != 1 {
		t.Errorf("expected 1 Discover call, got %d", calls)
	}
}

// TestGetInstances_SingleflightCoalesces 验证并发请求只触发一次 Discover。
func TestGetInstances_SingleflightCoalesces(t *testing.T) {
	disc := &countingDiscoverer{
		delay: 50 * time.Millisecond, // 模拟慢 Discover
		result: []registry.Instance{
			{IP: "10.0.0.1", Port: 8080},
		},
	}
	c := newTestClient(t, WithDiscoverer(disc))

	const concurrency = 20
	var wg sync.WaitGroup
	wg.Add(concurrency)
	start := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			<-start
			_, _ = c.GetInstances(context.Background(), "svc-sfg")
		}()
	}
	close(start)
	wg.Wait()

	// singleflight 应将并发请求合并为 1 次 Discover
	if calls := disc.getCalls(); calls != 1 {
		t.Errorf("expected 1 Discover call with singleflight, got %d", calls)
	}
}

// TestGetServiceBasic_FromDepService 验证从内存 DepService 读取配置。
func TestGetServiceBasic_FromDepService(t *testing.T) {
	c := newTestClient(t)
	dep := &metadata.ServiceBasic{
		ConnectionConfig: metadata.ConnectionConfig{
			ReadTimeout: 8000,
			Lb:          metadata.LbRandom,
		},
	}
	c.mu.Lock()
	c.depService["svc-dep"] = dep
	c.mu.Unlock()

	basic := c.GetServiceBasic("svc-dep")
	if basic.ReadTimeout != 8000 {
		t.Errorf("expected ReadTimeout 8000, got %d", basic.ReadTimeout)
	}
	if basic.Lb != metadata.LbRandom {
		t.Errorf("expected Lb %q, got %q", metadata.LbRandom, basic.Lb)
	}
}

// TestGetServiceBasic_NotLoaded_ReturnsDefault 验证未加载服务返回默认值。
func TestGetServiceBasic_NotLoaded_ReturnsDefault(t *testing.T) {
	c := newTestClient(t)

	basic := c.GetServiceBasic("svc-unknown")
	if basic.ReadTimeout != 5000 {
		t.Errorf("expected default ReadTimeout 5000, got %d", basic.ReadTimeout)
	}
	if basic.Lb != metadata.LbRoundRobin {
		t.Errorf("expected default Lb %q, got %q", metadata.LbRoundRobin, basic.Lb)
	}
}

// failingSubscriber 首次 Subscribe 返回错误，用于回滚行为测试。
type failingSubscriber struct {
	*registrymocks.Subscriber
	failOnce atomic.Bool
}

func (m *failingSubscriber) Subscribe(ctx context.Context, serviceName string, cb func([]registry.Instance)) error {
	if m.failOnce.CompareAndSwap(false, true) {
		return errors.New("transient nacos error")
	}
	return m.Subscriber.Subscribe(ctx, serviceName, cb)
}

// TestSubscribeService_ConcurrentRollbackIsolatesCallbacks 回归：A/B 并发
// 订阅同一服务时，整体快照回滚曾把 B 已成功注册的回调一并抹掉——之后
// 推送永远派发到空列表，B 静默收不到实例变更。钉住：失败方只摘除自身。
func TestSubscribeService_ConcurrentRollbackIsolatesCallbacks(t *testing.T) {
	sub := &failingSubscriber{Subscriber: registrymocks.NewSubscriber()}
	c := newTestClient(t, WithSubscriber(sub))

	var wgCallbacks sync.WaitGroup
	cbB := func([]registry.Instance) { wgCallbacks.Done() }

	// A：触发一次失败的 Subscribe（回滚路径）
	if err := c.SubscribeService(context.Background(), "svc-a", func([]registry.Instance) {}); err == nil {
		t.Fatal("expected first subscribe to fail")
	}
	// B：随后成功注册
	if err := c.SubscribeService(context.Background(), "svc-a", cbB); err != nil {
		t.Fatal(err)
	}

	// 推送必须能派发到 B 的回调（回滚曾把 instanceCallbacks 清成空）
	wgCallbacks.Add(1)
	sub.Push("svc-a", []registry.Instance{})
	waitDone(&wgCallbacks)
}

func waitDone(wg *sync.WaitGroup) {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		panic("callback not dispatched: rollback wiped other subscribers' callbacks")
	}
}
