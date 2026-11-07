package client

import (
	"context"

	"github.com/oekit/core/lifecycle"
	"github.com/oekit/registry"
)

// RunWithLifecycle 向注册中心注册当前服务实例,并通过 lifecycle.App 注册
// Deregister 作为优雅关闭时的 closer,业务无需手写 Register + Deregister 对称调用。
//
// # 调用顺序契约(#76):必须在 server 资源注册**之后**调用
//
// lifecycle.App 的 closer 按注册**逆序**执行。本方法注册的 deregister
// closer 要在关停时**先于** server.Shutdown 执行(先摘流量再停监听,对齐
// bootstrap/nacos 组件与 go-kratos Stop 语义),因此 deregister closer 必须
// **晚于** server 的 closer 注册——即先启动 server(hertz/kitex RunServer
// 内部经 app.AddResource 注册 server.Shutdown),再调本方法:
//
//	b, _ := bootstrap.New(bootstrap.WithServiceName("svc")) // + nacos/obs WithXxx 按需注入
//	b.Run(func(app *bootstrap.App) error {
//	    nnsc := nns.New(nacosBootstrap, opts...)  // 门面负责 provider 默认构造
//	    h := server.New(server.WithHostPorts(":8080"))
//	    hertz.RunServer(app, "hertz", h,
//	        hertz.WithOnReady(func(ctx context.Context) { app.Ready() }))
//	    // ⚠️ 必须在 RunServer 之后:server closer 先注册 ⇒ 关停逆序执行时
//	    // nns-deregister 先于 server.Shutdown(先摘流量,再停监听)。
//	    if err := nnsc.RunWithLifecycle(app, "my-svc", 8080); err != nil {
//	        return err
//	    }
//	    <-app.Context().Done() // 阻塞直到优雅关闭
//	    return nil
//	})
//
// 反例(旧示例的写法,勿用):先 RunWithLifecycle 再启动 server——关停时
// server 先停监听、实例才从注册中心摘除,窗口内 LB 仍把流量路由到已死的
// 实例。需要「注册严格等到 server 就绪」的语义时,用 bootstrap/nacos 的
// WithNNS/WithRequiredNNS(注册延迟到 app.Ready() 之后)。
//
// ⚠️ 行为:
//   - Register 失败:返回 error,business 决定是否继续(通常 return err 触发整体退出)
//   - Register 成功 + lifecycle 关闭:Deregister 在 bootstrap 关闭序列里被调,
//     走 lifecycle.App.AddResource 的逆序释放约定
//   - opts 同时传给 Register 和 Deregister,要求 opts 必须是幂等的(典型场景
//     只传 weight / metadata / group,业务不要传 random token 之类)
//
// 官方依据:ekit lifecycle 契约(lifecycle.App.AddResource)+ Kitex 官方
// graceful_shutdown 章节"注册中心注销"步骤
func (c *ServiceClient) RunWithLifecycle(app lifecycle.App, name string, port int, opts ...registry.RegisterOptionFunc) error {
	// name 是本服务名:优先沿用 LocalServiceInit 已记录的名字,
	// 未 Init 的调用路径(如 mock/降级)以本参数为准兜底
	c.setSelfServiceNameIfEmpty(name)
	// Register 用 app.Context()(启动期 ctx 未取消,注册受 app 生命周期管理)
	if err := c.Register(app.Context(), port, opts...); err != nil {
		return err
	}
	// ⚠️ Deregister 必须用 closer 收到的 shutdown ctx,不能用 app.Context()
	// (2026-09-05 复审修复):关闭序列触发时 app ctx 必然已取消,ctx-aware
	// 的 registrar 实现会带着已取消的 ctx 去注销,直接失败 — 恰恰违背本
	// helper 的存在目的。bootstrap 传入的 shutdown ctx 自带关闭预算超时。
	app.AddResource(name+"-nns-deregister", func(shutdownCtx context.Context) error {
		return c.Deregister(shutdownCtx, port, opts...)
	})
	return nil
}
