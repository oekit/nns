package client

import (
	"context"
	"errors"
	"testing"

	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// --- RegisterAs / DeregisterAs（nns#30）---

func TestRegisterAs(t *testing.T) {
	tests := []struct {
		name        string
		serviceName string
		port        int
		opts        []registry.RegisterOptionFunc
		wantErr     error
	}{
		{
			name:        "显式服务名",
			serviceName: "http-svc",
			port:        8080,
		},
		{
			name:        "完整 RPC 服务名",
			serviceName: "com.example.shop.order",
			port:        9090,
		},
		{
			name:        "带元数据与权重",
			serviceName: "http-svc",
			port:        8080,
			opts: []registry.RegisterOptionFunc{
				registry.WithMetadata(map[string]string{"version": "v1"}),
				registry.WithWeight(20),
			},
		},
		{
			name:        "服务名为空报错",
			serviceName: "",
			port:        8080,
			wantErr:     ErrServiceNameRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := &registrymocks.Registrar{}
			c := newTestClient(t, WithRegistrar(reg))

			err := c.RegisterAs(context.Background(), tt.serviceName, tt.port, tt.opts...)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				if len(reg.Registered) != 0 {
					t.Fatalf("expected no registration on error, got %d", len(reg.Registered))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(reg.Registered) != 1 {
				t.Fatalf("expected 1 registered instance, got %d", len(reg.Registered))
			}
			inst := reg.Registered[0]
			if inst.Service != tt.serviceName {
				t.Errorf("Service = %q, want %q", inst.Service, tt.serviceName)
			}
			if inst.Port != tt.port {
				t.Errorf("Port = %d, want %d", inst.Port, tt.port)
			}
			if len(tt.opts) > 0 {
				if inst.Metadata["version"] != "v1" {
					t.Errorf("Metadata[version] = %q, want v1", inst.Metadata["version"])
				}
				if inst.Weight != 20 {
					t.Errorf("Weight = %v, want 20", inst.Weight)
				}
			}
		})
	}
}

// RegisterAs 不要求先 Init——服务名由调用方显式给出。
func TestRegisterAs_NoInitRequired(t *testing.T) {
	reg := &registrymocks.Registrar{}
	c := newTestClient(t, WithRegistrar(reg))
	c.selfServiceName = "" // 未 Init

	if err := c.RegisterAs(context.Background(), "explicit-svc", 8080); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(reg.Registered) != 1 || reg.Registered[0].Service != "explicit-svc" {
		t.Fatalf("expected registration under explicit-svc, got %+v", reg.Registered)
	}
}

// 同一 SDK 连接注册多个服务名（HTTP + RPC 双端点形态）。
func TestRegisterAs_MultipleNames(t *testing.T) {
	reg := &registrymocks.Registrar{}
	c := newTestClient(t, WithRegistrar(reg))

	for _, tc := range []struct {
		name string
		port int
	}{
		{"http-svc", 8080},
		{"com.example.shop.order", 9090},
	} {
		if err := c.RegisterAs(context.Background(), tc.name, tc.port); err != nil {
			t.Fatalf("RegisterAs(%s): unexpected error: %v", tc.name, err)
		}
	}

	if len(reg.Registered) != 2 {
		t.Fatalf("expected 2 registered instances, got %d", len(reg.Registered))
	}
	if reg.Registered[0].Service != "http-svc" || reg.Registered[0].Port != 8080 {
		t.Errorf("first registration = %+v, want http-svc:8080", reg.Registered[0])
	}
	if reg.Registered[1].Service != "com.example.shop.order" || reg.Registered[1].Port != 9090 {
		t.Errorf("second registration = %+v, want com.example.shop.order:9090", reg.Registered[1])
	}
}

func TestDeregisterAs(t *testing.T) {
	tests := []struct {
		name        string
		serviceName string
		port        int
		wantErr     error
	}{
		{
			name:        "显式服务名",
			serviceName: "http-svc",
			port:        8080,
		},
		{
			name:        "服务名为空报错",
			serviceName: "",
			port:        8080,
			wantErr:     ErrServiceNameRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := &registrymocks.Registrar{}
			c := newTestClient(t, WithRegistrar(reg))

			err := c.DeregisterAs(context.Background(), tt.serviceName, tt.port)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(reg.Deregistered) != 1 {
				t.Fatalf("expected 1 deregistered instance, got %d", len(reg.Deregistered))
			}
			if reg.Deregistered[0].Service != tt.serviceName {
				t.Errorf("Service = %q, want %q", reg.Deregistered[0].Service, tt.serviceName)
			}
			if reg.Deregistered[0].Port != tt.port {
				t.Errorf("Port = %d, want %d", reg.Deregistered[0].Port, tt.port)
			}
		})
	}
}

// 未配置 registrar 时注销是幂等收尾（与 Deregister 一致），不是错误。
func TestDeregisterAs_NoRegistrar_NoError(t *testing.T) {
	c := newTestClient(t)
	if err := c.DeregisterAs(context.Background(), "http-svc", 8080); err != nil {
		t.Fatalf("expected nil error when registrar is nil, got: %v", err)
	}
}

func TestRegisterAs_NoRegistrar_NoOpSuccess(t *testing.T) {
	// #66 行为变更：no registrar = 幂等 no-op success（原断言期望
	// ErrRegistrarUnavailable——「没注册器」不是错误）。
	//
	// #126：把恒真断言 errors.Is(x, x)（实现是否返回哨兵都绿）换成行为
	// 断言——无 registrar 的注册/注销入口**恒不返回** ErrRegistrarUnavailable，
	// 哨兵仅兼容保留（#66 起任何 errors.Is(err, ErrRegistrarUnavailable)
	// 分支都不会命中）。
	c := &ServiceClient{}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"Register", c.Register(context.Background(), 8080)},
		{"RegisterAs", c.RegisterAs(context.Background(), "svc-x", 8080)},
		{"DeregisterAs", c.DeregisterAs(context.Background(), "svc-x", 8080)},
	} {
		if tc.err != nil {
			t.Fatalf("%s: no registrar should be no-op success, got %v", tc.name, tc.err)
		}
		if errors.Is(tc.err, ErrRegistrarUnavailable) {
			t.Fatalf("%s: must never return ErrRegistrarUnavailable after #66 (sentinel kept for compat only)", tc.name)
		}
	}
}

// TestRegisterAs_NoRegistrar_LogSaysRegister 钉住 #126 的复制粘贴错：
// **注册**路径的 no-op 日志此前写的是 "deregister is a no-op"。
func TestRegisterAs_NoRegistrar_LogSaysRegister(t *testing.T) {
	logs := captureLogs(t)
	c := &ServiceClient{}
	if err := c.RegisterAs(context.Background(), "svc-x", 8080); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !logs.contains("register is a no-op") {
		t.Fatalf("register path must log 'register is a no-op', captured: %v", logs.snapshot())
	}
	if logs.contains("deregister is a no-op") {
		t.Fatalf("register path must not log 'deregister is a no-op' (copy-paste bug, #126), captured: %v", logs.snapshot())
	}
}

// Registrar / Discoverer 逃生口（nns#30 顺带限制：门面不给 getter）。
func TestAccessors_RegistrarDiscoverer(t *testing.T) {
	reg := &registrymocks.Registrar{}
	disc := &registrymocks.Discoverer{}
	c := newTestClient(t, WithRegistrar(reg), WithDiscoverer(disc))

	if c.Registrar() != registry.Registrar(reg) {
		t.Error("Registrar() did not return the injected registrar")
	}
	if c.Discoverer() != registry.Discoverer(disc) {
		t.Error("Discoverer() did not return the injected discoverer")
	}

	empty := newTestClient(t)
	if empty.Registrar() != nil {
		t.Error("Registrar() should be nil when no registrar injected")
	}
	if empty.Discoverer() != nil {
		t.Error("Discoverer() should be nil when no discoverer injected")
	}
}

// --- 降级序可配置（nns#31）---

// TestGetInstances_FallbackOrder 覆盖静态实例与注册中心的两种优先序。
func TestGetInstances_FallbackOrder(t *testing.T) {
	static := []registry.Instance{{IP: "10.0.0.1", Port: 8080}}
	registryInsts := []registry.Instance{{IP: "10.0.0.2", Port: 9090}}

	tests := []struct {
		name      string
		opts      []ClientOption
		wantIP    string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "静态优先：静态短路注册中心",
			opts:      []ClientOption{WithStaticInstances("svc", static), WithDiscoverer(&registrymocks.Discoverer{Instances: registryInsts})},
			wantIP:    "10.0.0.1",
			wantCount: 1,
		},
		{
			name:      "注册中心优先：注册中心有实例时不用静态",
			opts:      []ClientOption{WithFallbackOrder(FallbackRegistryFirst), WithStaticInstances("svc", static), WithDiscoverer(&registrymocks.Discoverer{Instances: registryInsts})},
			wantIP:    "10.0.0.2",
			wantCount: 1,
		},
		{
			name:      "注册中心优先：注册中心为空时静态兜底",
			opts:      []ClientOption{WithFallbackOrder(FallbackRegistryFirst), WithStaticInstances("svc", static), WithDiscoverer(&registrymocks.Discoverer{})},
			wantIP:    "10.0.0.1",
			wantCount: 1,
		},
		{
			name:      "注册中心优先：注册中心报错且无缓存时静态兜底",
			opts:      []ClientOption{WithFallbackOrder(FallbackRegistryFirst), WithStaticInstances("svc", static), WithDiscoverer(&registrymocks.Discoverer{Err: errors.New("nacos down")})},
			wantIP:    "10.0.0.1",
			wantCount: 1,
		},
		{
			name:      "注册中心优先：无静态实例时注册中心失败仍报错",
			opts:      []ClientOption{WithFallbackOrder(FallbackRegistryFirst), WithDiscoverer(&registrymocks.Discoverer{Err: errors.New("nacos down")})},
			wantErr:   true,
			wantCount: 0,
		},
		{
			name:      "兜底语义：单服务指定，注册中心有实例时不用静态",
			opts:      []ClientOption{WithStaticFallback("svc", static), WithDiscoverer(&registrymocks.Discoverer{Instances: registryInsts})},
			wantIP:    "10.0.0.2",
			wantCount: 1,
		},
		{
			name:      "兜底语义：单服务指定，注册中心为空时静态兜底",
			opts:      []ClientOption{WithStaticFallback("svc", static), WithDiscoverer(&registrymocks.Discoverer{})},
			wantIP:    "10.0.0.1",
			wantCount: 1,
		},
		{
			name:      "兜底语义：其他服务不受影响（仍短路）",
			opts:      []ClientOption{WithStaticFallback("other", static), WithStaticInstances("svc", static), WithDiscoverer(&registrymocks.Discoverer{Instances: registryInsts})},
			wantIP:    "10.0.0.1",
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient(t, tt.opts...)

			instances, err := c.GetInstances(context.Background(), "svc")
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(instances) != tt.wantCount {
				t.Fatalf("expected %d instances, got %d", tt.wantCount, len(instances))
			}
			if instances[0].IP != tt.wantIP {
				t.Errorf("IP = %s, want %s", instances[0].IP, tt.wantIP)
			}
		})
	}
}

// 兜底语义下，注册中心返回空后第二次调用仍走兜底（不被空缓存绕过）。
func TestGetInstances_StaticFallback_EmptyCacheDoesNotBypass(t *testing.T) {
	static := []registry.Instance{{IP: "10.0.0.1", Port: 8080}}
	disc := &registrymocks.Discoverer{}
	c := newTestClient(t, WithStaticFallback("svc", static), WithDiscoverer(disc))

	for i := range 2 {
		instances, err := c.GetInstances(context.Background(), "svc")
		if err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
		if len(instances) != 1 || instances[0].IP != "10.0.0.1" {
			t.Fatalf("call %d: expected static fallback, got %+v", i, instances)
		}
	}
}

// 兜底语义下，注册中心有实例时静态地址不得遮蔽它（实例换 IP 后仍跟注册中心）。
func TestGetInstances_StaticFallback_RegistryWins(t *testing.T) {
	static := []registry.Instance{{IP: "10.0.0.1", Port: 8080}}
	disc := &registrymocks.Discoverer{Instances: []registry.Instance{{IP: "10.0.0.99", Port: 8080}}}
	c := newTestClient(t, WithStaticFallback("svc", static), WithDiscoverer(disc))

	instances, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 1 || instances[0].IP != "10.0.0.99" {
		t.Fatalf("expected registry instance 10.0.0.99, got %+v", instances)
	}
}

// 同一服务上重复配置以最后一次调用为准。
func TestStaticOptions_LastCallWins(t *testing.T) {
	static := []registry.Instance{{IP: "10.0.0.1", Port: 8080}}
	registryInsts := []registry.Instance{{IP: "10.0.0.2", Port: 9090}}
	disc := &registrymocks.Discoverer{Instances: registryInsts}

	tests := []struct {
		name   string
		opts   []ClientOption
		wantIP string
	}{
		{
			name:   "先兜底后短路 → 短路",
			opts:   []ClientOption{WithStaticFallback("svc", static), WithStaticInstances("svc", static), WithDiscoverer(disc)},
			wantIP: "10.0.0.1",
		},
		{
			name:   "先短路后兜底 → 兜底",
			opts:   []ClientOption{WithStaticInstances("svc", static), WithStaticFallback("svc", static), WithDiscoverer(disc)},
			wantIP: "10.0.0.2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient(t, tt.opts...)
			instances, err := c.GetInstances(context.Background(), "svc")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(instances) != 1 || instances[0].IP != tt.wantIP {
				t.Fatalf("expected IP %s, got %+v", tt.wantIP, instances)
			}
		})
	}
}

// 降级序字符串表示。
func TestFallbackOrder_String(t *testing.T) {
	if got := FallbackStaticFirst.String(); got != "static-first" {
		t.Errorf("FallbackStaticFirst.String() = %q, want static-first", got)
	}
	if got := FallbackRegistryFirst.String(); got != "registry-first" {
		t.Errorf("FallbackRegistryFirst.String() = %q, want registry-first", got)
	}
}

// Mock 模式对未配静态实例的名字返回「空列表 + nil error」——
// 空列表表示「当前无可用实例，由调用方决定回落」，不是错误。
func TestGetInstances_MockMode_EmptyIsNotError(t *testing.T) {
	c := newTestClient(t, WithMockMode(true))

	instances, err := c.GetInstances(context.Background(), "unknown-svc")
	if err != nil {
		t.Fatalf("mock mode should not error on unknown service, got: %v", err)
	}
	if len(instances) != 0 {
		t.Fatalf("expected empty instances, got %d", len(instances))
	}
}

// 兜底语义在 Mock 模式下同样生效（静态是唯一数据源）。
func TestGetInstances_MockMode_StaticFallback(t *testing.T) {
	static := []registry.Instance{{IP: "10.0.0.1", Port: 8080}}
	c := newTestClient(t, WithMockMode(true), WithStaticFallback("svc", static))

	instances, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 1 || instances[0].IP != "10.0.0.1" {
		t.Fatalf("expected static fallback, got %+v", instances)
	}
}

// #149 快照契约（原 TestStaticFallbackPool_ReturnsCopy 的「返回私有拷贝」
// 契约随热路径零拷贝化收紧为「返回只读快照」，见 GetInstances godoc）：
// 本组用例钉住 RCU 的不变式——本包绝不原地改写已发布快照，更新整体替换。
func TestReturnedSnapshots_UpdatesReplaceWholesale(t *testing.T) {
	c := newTestClient(t, WithDiscoverer(&registrymocks.Discoverer{}))
	c.updateCache("svc", []registry.Instance{
		{IP: "10.0.0.1", Port: 8080, Metadata: map[string]string{"k": "v"}},
	})
	ctx := context.Background()

	first, err := c.GetInstances(ctx, "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 推送更新：整体替换，旧快照内容恒定（持有旧引用的调用方不受影响）
	c.updateCache("svc", []registry.Instance{{IP: "10.0.0.2", Port: 8080}})
	if first[0].IP != "10.0.0.1" || first[0].Metadata["k"] != "v" {
		t.Fatalf("#149 RCU 违约：旧快照被原地改写（got IP=%s meta=%v）", first[0].IP, first[0].Metadata)
	}

	second, err := c.GetInstances(ctx, "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(second) != 1 || second[0].IP != "10.0.0.2" {
		t.Fatalf("新读取应可见更新后的快照, got %+v", second)
	}
}

func TestStaticSnapshots_AppendPublishesFreshSlice(t *testing.T) {
	static := []registry.Instance{{IP: "10.0.0.1", Port: 8080, Metadata: map[string]string{"k": "v"}}}
	c := newTestClient(t, WithStaticFallback("svc", static))
	ctx := context.Background()

	first, err := c.GetInstances(ctx, "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 追加写入发布全新切片：先前发出的快照不被原地改写
	c.AddStaticFallback("svc", registry.Instance{IP: "10.0.0.2", Port: 8080})
	if len(first) != 1 || first[0].IP != "10.0.0.1" || first[0].Metadata["k"] != "v" {
		t.Fatalf("#149 RCU 违约：静态追加污染了已发布快照, got %+v", first)
	}

	again, err := c.GetInstances(ctx, "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(again) != 2 {
		t.Fatalf("追加后应可见 2 个实例, got %+v", again)
	}
}

// 兜底静态实例不应遮蔽新鲜缓存。
func TestGetInstances_StaticFallback_FreshCacheWins(t *testing.T) {
	static := []registry.Instance{{IP: "10.0.0.1", Port: 8080}}
	c := newTestClient(t, WithStaticFallback("svc", static), WithDiscoverer(&registrymocks.Discoverer{}))
	c.updateCache("svc", []registry.Instance{{IP: "10.0.0.5", Port: 7070}})

	instances, err := c.GetInstances(context.Background(), "svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instances) != 1 || instances[0].IP != "10.0.0.5" {
		t.Fatalf("expected cached instance 10.0.0.5, got %+v", instances)
	}
}
