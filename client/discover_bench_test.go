package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/oekit/nns/metadata"
	"github.com/oekit/registry"
	"github.com/oekit/registry/registrymocks"
)

// benchClient 构造与 newTestClient 同口径的客户端（基准里不能传 *testing.T）。
func benchClient(opts ...ClientOption) *ServiceClient {
	c := &ServiceClient{
		configGroup:        "DEFAULT_GROUP",
		selfServiceName:    "bench-service",
		depService:         make(map[string]*metadata.ServiceBasic),
		instanceCallbacks:  make(map[string][]*instanceCallback),
		subscribedServices: make(map[string]bool),
		instanceCache:      make(map[string][]registry.Instance),
		cacheLastUpdate:    make(map[string]time.Time),
		cacheExpiry:        30 * time.Second,
		staticInstances:    make(map[string][]registry.Instance),
		fallbackOnly:       make(map[string]bool),
		localServiceConfig: make(map[string]*metadata.ServiceBasic),
	}
	for _, opt := range opts {
		opt(c)
	}
	initMeta := make(map[string]string)
	c.localServiceMeta.Store(&initMeta)
	return c
}

// benchInstances 构造 n 实例 × 每实例 metaKeys 个元数据键的实例表
// （nns-endpoint 每请求 GetInstances 的量级：大实例表 × 高 QPS）。
func benchInstances(n, metaKeys int) []registry.Instance {
	instances := make([]registry.Instance, n)
	for i := range instances {
		md := make(map[string]string, metaKeys)
		for k := 0; k < metaKeys; k++ {
			md[fmt.Sprintf("meta-key-%d", k)] = fmt.Sprintf("meta-value-%d", k)
		}
		instances[i] = registry.Instance{
			Service:  "bench-svc",
			IP:       fmt.Sprintf("10.%d.%d.%d", i>>16&0xff, i>>8&0xff, i&0xff),
			Port:     8080,
			Weight:   10,
			Metadata: md,
		}
	}
	return instances
}

// BenchmarkGetInstances_CacheHit_10x2 / _100x4 钉住 #149 的热路径改善：
// 缓存命中路径在 RCU 读（#149）前每请求深拷贝实例列表 + 每实例一个
// Metadata map（10 实例 = 11 allocs/op，100 实例 × 4 键 = 101 allocs/op）；
// 改造后命中路径零分配（返回已发布只读快照）。
func BenchmarkGetInstances_CacheHit_10x2(b *testing.B) {
	benchmarkGetInstancesCacheHit(b, 10, 2)
}

func BenchmarkGetInstances_CacheHit_100x4(b *testing.B) {
	benchmarkGetInstancesCacheHit(b, 100, 4)
}

func benchmarkGetInstancesCacheHit(b *testing.B, n, metaKeys int) {
	c := benchClient(WithDiscoverer(&registrymocks.Discoverer{}))
	c.updateCache("bench-svc", benchInstances(n, metaKeys))
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		instances, err := c.GetInstances(ctx, "bench-svc")
		if err != nil || len(instances) != n {
			b.Fatalf("GetInstances: err=%v len=%d want %d", err, len(instances), n)
		}
	}
}

// BenchmarkGetInstances_StaticShortCircuit 静态短路路径（同样零拷贝化）。
func BenchmarkGetInstances_StaticShortCircuit(b *testing.B) {
	c := benchClient(WithStaticInstances("bench-svc", benchInstances(100, 4)))
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		instances, err := c.GetInstances(ctx, "bench-svc")
		if err != nil || len(instances) != 100 {
			b.Fatalf("GetInstances: err=%v len=%d", err, len(instances))
		}
	}
}
