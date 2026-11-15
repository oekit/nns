package nns

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/oekit/logx"
	nacospkg "github.com/oekit/registry/nacos"
)

// captureLogger 实现 logx.Logger：把日志行留在内存供断言（#210——appendNacosDefaults
// 两段构造是否都被尝试，只能从 Warn 日志判定）。未覆盖的方法继承 logx.Nop()
// （空转）；With* 派生返回自身，保证 logx.Delegating 的
// Default().WithAttrs(...) 解析链仍落回本捕获器（与 client 包测试同款形态）。
type captureLogger struct {
	logx.Logger // Nop 基底
	mu          sync.Mutex
	lines       []string
}

func (c *captureLogger) Info(msg string, kv ...any)  { c.record("INFO", msg, kv) }
func (c *captureLogger) Warn(msg string, kv ...any)  { c.record("WARN", msg, kv) }
func (c *captureLogger) Error(msg string, kv ...any) { c.record("ERROR", msg, kv) }

func (c *captureLogger) record(level, msg string, kv []any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, level+" "+msg+" "+fmt.Sprint(kv...))
}

func (c *captureLogger) WithAttrs(...logx.Attr) logx.Logger  { return c }
func (c *captureLogger) WithGroup(string) logx.Logger        { return c }
func (c *captureLogger) WithService(string) logx.Logger      { return c }
func (c *captureLogger) WithCtx(context.Context) logx.Logger { return c }

func (c *captureLogger) contains(substr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, l := range c.lines {
		if strings.Contains(l, substr) {
			return true
		}
	}
	return false
}

func (c *captureLogger) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}

// captureLogs 把进程默认 logger 换成捕获器，测试结束自动还原。
func captureLogs(t *testing.T) *captureLogger {
	t.Helper()
	c := &captureLogger{Logger: logx.Nop()}
	prev := logx.Default()
	logx.SetDefault(c)
	t.Cleanup(func() { logx.SetDefault(prev) })
	return c
}

// TestAppendNacosDefaults_ConfigAttemptedAfterNamingFailure 回归（#210）：
// naming 与 config 是两次彼此独立的 SDK 构造（各自持有 gRPC 连接与后台
// goroutine、各自有 closer）——naming 失败只降级 naming 面，config 段必须
// 继续执行。修复前 naming 失败 early return 把后半段整段跳过：ServiceConfig
// 恒回 ErrConfigStoreUnavailable、WatchService 因 configStore==nil 直接
// no-op，配置拉取/热更整面静默失效，而日志只提 cache/static、不提配置中心。
func TestAppendNacosDefaults_ConfigAttemptedAfterNamingFailure(t *testing.T) {
	logs := captureLogs(t)

	// 空 addr：两次 SDK 构造都在 buildNacosConfig 快速失败（parseAddr 报错），
	// 不会创建 SDK client 或起后台 goroutine——本用例只钉控制流与日志面。
	_, _ = appendNacosDefaults(nil, nacospkg.Bootstrap{})

	if !logs.contains("create naming client failed") {
		t.Fatalf("naming 构造失败应记 Warn, captured: %v", logs.snapshot())
	}
	if !logs.contains("create config client failed") {
		t.Fatalf("#210 回归：naming 失败后 config 段被整段跳过（配置面静默失效）, captured: %v",
			logs.snapshot())
	}
}
