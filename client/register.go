package client

import (
	"context"
	"fmt"
	"github.com/oekit/core/network"
	"github.com/oekit/registry"
)

// Register 向注册中心注册当前服务实例。
//
// 服务名取 LocalServiceInit 记录的自身服务名（须先 Init），使用
// GetPodIP 探测本机 IP。opts 控制权重、元数据等注册选项。
func (c *ServiceClient) Register(ctx context.Context, port int, opts ...registry.RegisterOptionFunc) error {
	if c.registrar == nil {
		return ErrRegistrarUnavailable
	}
	name := c.getSelfServiceName()
	if name == "" {
		return fmt.Errorf("nns/client: service name unknown; call Init/LocalServiceInit before Register")
	}

	return c.registrar.Register(ctx, c.buildInstance(name, port, opts...))
}

// RegisterAs 向注册中心注册当前服务实例到**指定服务名**下。
//
// 与 Register 的区别只有一处：服务名由调用方显式给出，而不是取
// LocalServiceInit 记录的自身服务名。因此 RegisterAs **不要求先 Init**。
//
// 一个服务对外暴露多个端点（HTTP + RPC 双协议、分别注册在短名与完整
// RPC 服务名下）时，用同一个 SDK 连接把每个名字各注册一次即可——不必
// 为每个名字各开一个门面实例（那会翻倍 Nacos gRPC 长连接与后台 goroutine）。
//
// 端口各自传入（同一进程的两个端点端口不同）。IP 与 Register 一致，取
// GetPodIP 探测的本机 IP。opts 控制权重、元数据等注册选项。
//
// 配套的注销入口是 DeregisterAs：注册了几个名字就要注销几个名字，
// 否则注册中心里会留下无人回收的实例。
//
// 未配置 registrar 时返回 ErrRegistrarUnavailable；服务名为空时返回
// ErrServiceNameRequired（参数校验先于环境检查——空服务名是调用方的错，
// 报「registrar 不可用」会把它掩盖掉）。
func (c *ServiceClient) RegisterAs(ctx context.Context, serviceName string, port int, opts ...registry.RegisterOptionFunc) error {
	if serviceName == "" {
		return ErrServiceNameRequired
	}
	if c.registrar == nil {
		return ErrRegistrarUnavailable
	}

	return c.registrar.Register(ctx, c.buildInstance(serviceName, port, opts...))
}

// Deregister 从注册中心注销当前服务实例。
func (c *ServiceClient) Deregister(ctx context.Context, port int, opts ...registry.RegisterOptionFunc) error {
	if c.registrar == nil {
		return nil
	}

	return c.registrar.Deregister(ctx, c.buildInstance(c.getSelfServiceName(), port, opts...))
}

// DeregisterAs 从注册中心注销指定服务名下的当前服务实例。
//
// 与 RegisterAs 配套：注销的实例与服务名、端口必须与注册时一致，
// 否则注册中心匹配不到实例、注销会静默失效。
//
// 未配置 registrar 时返回 nil（与 Deregister 一致：注销是幂等收尾，
// 没有 registrar 说明根本没注册过）；服务名为空时返回 ErrServiceNameRequired
// （参数校验先于环境检查）。
func (c *ServiceClient) DeregisterAs(ctx context.Context, serviceName string, port int, opts ...registry.RegisterOptionFunc) error {
	if serviceName == "" {
		return ErrServiceNameRequired
	}
	if c.registrar == nil {
		return nil
	}

	return c.registrar.Deregister(ctx, c.buildInstance(serviceName, port, opts...))
}

// buildInstance 构造注册实例：服务名与端口由调用方给出，IP 取本机
// Pod IP，权重/元数据/分组/集群取自 opts。
func (c *ServiceClient) buildInstance(serviceName string, port int, opts ...registry.RegisterOptionFunc) registry.Instance {
	opt := registry.Apply(opts...)
	return registry.Instance{
		Service:  serviceName,
		IP:       network.GetPodIP(),
		Port:     port,
		Weight:   opt.Weight,
		Metadata: opt.Metadata,
		Group:    opt.Group,
		Cluster:  opt.Cluster,
	}
}
