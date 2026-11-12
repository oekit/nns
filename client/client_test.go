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

func TestRegister_NoRegistrar_NoOpSuccess(t *testing.T) {
	// #66 行为变更：「没注册器」（mock/未启用）= 幂等 no-op success——
	// 「本来就没注册」不是故障，消费方不再需要自维护门控布尔。
	c := &ServiceClient{}
	if err := c.Register(context.Background(), 8080); err != nil {
		t.Fatalf("no registrar should be no-op success, got %v", err)
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

// TestNew_DefaultCacheExpiry 钉住 nns#94：client.New() 直连使用（provider
// 无关用法）时 cacheExpiry 零值必须回填缺省 30s（与门面 nns.New 同缺省），
// 而不是让容错层拿到 0——cacheExpiry==0 时 time.Since(lastUpdate) > 0 恒
// 成立，「新鲜缓存」（Discover 容错链第 3 级）写入即过期、每次直通注册中心。
// 显式 0 / 负值同样回填（过期语义没有「禁用」开关），显式正值原样保留。
func TestNew_DefaultCacheExpiry(t *testing.T) {
	tests := []struct {
		name string
		opts []ClientOption
		want time.Duration
	}{
		{"零值缺省", nil, 30 * time.Second},
		{"显式 0 回填缺省", []ClientOption{WithCacheExpiry(0)}, 30 * time.Second},
		{"显式负值回填缺省", []ClientOption{WithCacheExpiry(-time.Second)}, 30 * time.Second},
		{"显式正值保留", []ClientOption{WithCacheExpiry(5 * time.Minute)}, 5 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New(tt.opts...)
			if c.cacheExpiry != tt.want {
				t.Errorf("New().cacheExpiry = %v, want %v", c.cacheExpiry, tt.want)
			}
		})
	}
}

// TestNew_ZeroCacheExpiry_FreshCacheServes 钉住 nns#94 的行为面：直连
// client.New()（不传 cacheExpiry）时第二次 GetInstances 必须由「新鲜缓存」
// 服务，不再穿透到注册中心。变异性负对照：还原 New 的缺省回填后，
// discoverer 调用次数变 2、本测试转红。
func TestNew_ZeroCacheExpiry_FreshCacheServes(t *testing.T) {
	d := &countingDiscoverer{result: []registry.Instance{{IP: "10.0.0.1", Port: 8080}}}
	c := New(WithDiscoverer(d))
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		insts, err := c.GetInstances(ctx, "svc-fresh")
		if err != nil {
			t.Fatalf("GetInstances #%d: %v", i+1, err)
		}
		if len(insts) != 1 || insts[0].IP != "10.0.0.1" {
			t.Fatalf("GetInstances #%d = %v, want 1 instance 10.0.0.1", i+1, insts)
		}
		// 隔开两次调用的时钟读数：修复前 cacheExpiry==0 会让第 2 次调用
		// 因 time.Since(lastUpdate) > 0 判过期而穿透
		time.Sleep(2 * time.Millisecond)
	}

	if got := d.getCalls(); got != 1 {
		t.Errorf("discoverer called %d times, want 1（第二次应由新鲜缓存服务，不应穿透注册中心）", got)
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

// TestDeregister_EmptySelfName_ReturnsErr 回归 #153：Deregister 对空服务名
// 不校验、与 DeregisterAs/Register 不对称——未 Init 时以 Service="" 现算
// 实例打给 provider，调用方拿不到「没先 Init」的根因信号。修复后与
// DeregisterAs 同序：参数校验（空名 → ErrServiceNameRequired）先于环境
// 检查（无 registrar 的 no-op）。
func TestDeregister_EmptySelfName_ReturnsErr(t *testing.T) {
	// 未 Init 且无 registrar：参数校验先于环境检查，仍返回 ErrServiceNameRequired
	c := New()
	if err := c.Deregister(context.Background(), 8080); !errors.Is(err, ErrServiceNameRequired) {
		t.Fatalf("#153 regression: expected ErrServiceNameRequired for empty self name, got %v", err)
	}
	// 未 Init 但有 registrar：同一校验、同一错误
	c2 := New(WithRegistrar(&registrymocks.Registrar{}))
	if err := c2.Deregister(context.Background(), 8080); !errors.Is(err, ErrServiceNameRequired) {
		t.Fatalf("#153 regression: expected ErrServiceNameRequired with registrar but no Init, got %v", err)
	}
	// 对照组：Init 后（服务名已记录）注销走正常路径（无 registrar = no-op nil）
	c3 := New()
	c3.setSelfServiceNameIfEmpty("init-svc")
	if err := c3.Deregister(context.Background(), 8080); err != nil {
		t.Fatalf("deregister with known name should succeed (no registrar = no-op), got %v", err)
	}
}

// TestGetInstances_SingleflightWaitersSharePublishedSnapshot 钉住 #151 的
// 现行契约（#155 rework 后）：singleflight 等待方共享同一份「已发布只读
// 快照」——热路径零拷贝（#149）以「调用方不得修改返回实例」的快照契约
// 替代按调用方拷贝（每请求深拷贝实例列表 + Metadata map 是 nns-endpoint
// 每请求调用的稳态分配源，BenchmarkGetInstances_CacheHit_* 钉分配数）。
//
// 本测试断言全部调用方（lead + 等待方）拿到的 backing array 元素地址一致
// （共享同一只读快照）；谁把等待方改回按调用方拷贝，必须连带修订 #149
// 的快照契约与 bench 口径——此断言就是把该决定显式钉住的证物。
func TestGetInstances_SingleflightWaitersSharePublishedSnapshot(t *testing.T) {
	disc := &countingDiscoverer{
		delay: 50 * time.Millisecond, // 慢 Discover：让并发调用落入同一 flight
		result: []registry.Instance{
			{IP: "10.0.0.1", Port: 8080, Metadata: map[string]string{"v": "1"}},
		},
	}
	c := newTestClient(t, WithDiscoverer(disc))

	const callers = 4
	results := make([][]registry.Instance, callers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			insts, err := c.GetInstances(context.Background(), "svc-share")
			if err != nil {
				t.Errorf("GetInstances #%d: %v", i, err)
				return
			}
			results[i] = insts
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < callers; i++ {
		if len(results[i]) != 1 {
			t.Fatalf("caller %d got empty result", i)
		}
		if &results[i][0] != &results[0][0] {
			t.Fatalf("#151: caller %d got a distinct copy of the instances slice; "+
				"want the shared published read-only snapshot (#149 zero-copy contract)", i)
		}
	}
	if calls := disc.getCalls(); calls != 1 {
		t.Errorf("discover called %d times, want 1 (singleflight)", calls)
	}
}
