package client

import (
	"context"
	"fmt"
	"github.com/oekit/core/network"
	"github.com/oekit/registry/registry"
)

// Register 向注册中心注册当前服务实例。
//
// 服务名取 LocalServiceInit 记录的自身服务名（须先 Init），使用
// GetPodIP 探测本机 IP。opts 控制权重、元数据等注册选项。
func (c *ServiceClient) Register(ctx context.Context, port int, opts ...registry.RegisterOptionFunc) error {
	if c.registrar == nil {
		return ErrRegistrarUnavailable
	}
	if name := c.getSelfServiceName(); name == "" {
		return fmt.Errorf("nns/client: service name unknown; call Init/LocalServiceInit before Register")
	}

	opt := registry.Apply(opts...)
	inst := registry.Instance{
		Service:  c.getSelfServiceName(),
		IP:       network.GetPodIP(),
		Port:     port,
		Weight:   opt.Weight,
		Metadata: opt.Metadata,
		Group:    opt.Group,
		Cluster:  opt.Cluster,
	}
	return c.registrar.Register(ctx, inst)
}

// Deregister 从注册中心注销当前服务实例。
func (c *ServiceClient) Deregister(ctx context.Context, port int, opts ...registry.RegisterOptionFunc) error {
	if c.registrar == nil {
		return nil
	}

	opt := registry.Apply(opts...)
	inst := registry.Instance{
		Service:  c.getSelfServiceName(),
		IP:       network.GetPodIP(),
		Port:     port,
		Weight:   opt.Weight,
		Metadata: opt.Metadata,
		Group:    opt.Group,
		Cluster:  opt.Cluster,
	}
	return c.registrar.Deregister(ctx, inst)
}
