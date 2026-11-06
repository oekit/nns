package client

import (
	"context"
	"fmt"

	"github.com/oekit/core/network"
	"github.com/oekit/logx"
	"github.com/oekit/registry"
)

var log = logx.Delegating("nns/client")

// Register 向注册中心注册当前服务实例。
//
// 服务名取 LocalServiceInit 记录的自身服务名（须先 Init），使用
// GetPodIP 探测本机 IP。opts 控制权重、元数据等注册选项。
func (c *ServiceClient) Register(ctx context.Context, port int, opts ...registry.RegisterOptionFunc) error {
	if c.registrar == nil {
		// #66：「没注册器」（mock 模式/未启用注册发现）不是错误——幂等 no-op
		// success，与 nns.enabled:false 的「不注册」语义对齐（mock 开箱即用）；
		// 真故障（Nacos 连不上）走 registrar 内部 error 路径仍可区分。消费方
		// 不再需要自维护 Enabled&&!Mock 门控布尔并保证注册/注销对称。
		log.Info("no registrar configured (mock or discovery disabled), register is a no-op",
			"port", port)
		return nil
	}
	name := c.getSelfServiceName()
	if name == "" {
		return fmt.Errorf("nns/client: service name unknown; call Init/LocalServiceInit before Register")
	}

	inst, err := c.buildInstance(name, port, opts...)
	if err != nil {
		return err
	}
	return c.registrar.Register(ctx, inst)
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
		// #66：同 Register——no-op success（幂等，注销对称性不再依赖调用方纪律）
		log.Info("no registrar configured (mock or discovery disabled), deregister is a no-op")
		return nil
	}

	inst, err := c.buildInstance(serviceName, port, opts...)
	if err != nil {
		return err
	}
	return c.registrar.Register(ctx, inst)
}

// Deregister 从注册中心注销当前服务实例。
func (c *ServiceClient) Deregister(ctx context.Context, port int, opts ...registry.RegisterOptionFunc) error {
	if c.registrar == nil {
		return nil
	}

	inst, err := c.buildInstance(c.getSelfServiceName(), port, opts...)
	if err != nil {
		return err
	}
	return c.registrar.Deregister(ctx, inst)
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

	inst, err := c.buildInstance(serviceName, port, opts...)
	if err != nil {
		return err
	}
	return c.registrar.Deregister(ctx, inst)
}

// buildInstance 构造注册实例：服务名与端口由调用方给出，IP 取本机
// Pod IP，权重/元数据/分组/集群取自 opts。
func (c *ServiceClient) buildInstance(serviceName string, port int, opts ...registry.RegisterOptionFunc) (registry.Instance, error) {
	opt := registry.Apply(opts...)
	ip := network.GetPodIP()
	// #68：注册路径拒绝回环地址——GetPodIP 的 127.0.0.1 静默回退（网卡枚举
	// 失败）在注册消费链上必须显式失败：注册一个永远不可达的回环地址比注册
	// 失败更坏（discover 拿到拨不通的实例，根因被彻底掩盖）。
	if ip == "" || ip == "127.0.0.1" || ip == "::1" {
		return registry.Instance{}, fmt.Errorf(
			"nns/client: refusing to register loopback/empty Pod IP %q for %s (nic enumeration likely failed; set host ip explicitly)", ip, serviceName)
	}
	return registry.Instance{
		Service:  serviceName,
		IP:       ip,
		Port:     port,
		Weight:   opt.Weight,
		Metadata: opt.Metadata,
		Group:    opt.Group,
		Cluster:  opt.Cluster,
	}, nil
}
