package client

import (
	"context"

	"github.com/yi-nology/ekit-core/lifecycle"
	"github.com/yi-nology/ekit-registry/registry"
)

// RunWithLifecycle 向注册中心注册当前服务实例,并通过 lifecycle.App 注册
// Deregister 作为优雅关闭时的 closer,业务无需手写 Register + Deregister 对称调用。
//
// 业务使用(在 bootstrap.Run userFn 内):
//
//	bootstrap.Run(cfg, func(app *bootstrap.App) error {
//	    nnsc := nns.New(nacosBootstrap, opts...)  // 门面负责 provider 默认构造
//	    if err := nnsc.RunWithLifecycle(app, "my-svc", 8080); err != nil {
//	        return err
//	    }
//	    // ... 启动 server,等待优雅关闭
//	    return nil
//	})
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
