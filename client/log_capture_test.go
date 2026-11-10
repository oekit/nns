package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/oekit/logx"
)

// captureLogger 实现 logx.Logger：把 Info/Warn 记录进内存供断言
// （#126 注册路径日志文本、#128 切路径 Warn）。未覆盖的方法继承
// logx.Nop()（空转）；With*/SetLevel 返回自身，保证 logx.Delegating 的
// Default().WithAttrs(...) 解析链仍落回本捕获器。
type captureLogger struct {
	logx.Logger // Nop 基底
	mu          sync.Mutex
	lines       []string
}

func (c *captureLogger) Info(msg string, kv ...any) { c.record("INFO", msg, kv) }
func (c *captureLogger) Warn(msg string, kv ...any) { c.record("WARN", msg, kv) }

func (c *captureLogger) record(level, msg string, kv []any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, level+" "+msg+" "+fmt.Sprint(kv...))
}

// With* 派生返回自身：Delegating 先 Default().WithAttrs(component) 再打日志，
// 返回 Nop 派生会丢捕获。
func (c *captureLogger) WithAttrs(...logx.Attr) logx.Logger { return c }
func (c *captureLogger) WithGroup(string) logx.Logger      { return c }
func (c *captureLogger) WithService(string) logx.Logger    { return c }
func (c *captureLogger) WithCtx(context.Context) logx.Logger {
	return c
}

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
