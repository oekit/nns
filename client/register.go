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
//
// 注册成功后实例身份（IP/Group/Cluster 等）会被缓存（#75）：配套的
// Deregister 优先用该快照注销，注册与注销之间 GetPodIP 结果变化、或注销时
// 漏传 WithGroup/WithCluster，都不会再让注销静默失效。
//
// 未配置 registrar（mock/未启用注册发现）时是幂等 no-op success（#66），
// 不返回错误——「本来就没注册」不是故障；no-op 判定先于服务名检查
// （#142 收敛前后的行为保持：无 registrar 时即使未 Init 也返回 nil）。
func (c *ServiceClient) Register(ctx context.Context, port int, opts ...registry.RegisterOptionFunc) error {
	// no-op 判定先于服务名检查：no registrar 时即使服务名未知也是 no-op
	// success（#66，TestRegister_NoRegistrar_NoOpSuccess 钉住零值客户端）。
	if c.registrar == nil {
		c.logRegisterNoOp(port)
		return nil
	}
	name := c.getSelfServiceName()
	if name == "" {
		return fmt.Errorf("nns/client: service name unknown; call Init/LocalServiceInit before Register")
	}
	return c.registerAs(ctx, name, port, opts...)
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
// 注册成功后实例身份会被缓存（#75），DeregisterAs 优先用快照注销。
//
// 未配置 registrar（mock/未启用注册发现）时是幂等 no-op success（#66），
// 不返回错误——与 Register 一致，注销对称性不再依赖调用方纪律。
// ErrRegistrarUnavailable 自 #66 起**不再被任何入口返回**（仅兼容保留符号，
// 见其声明），任何 errors.Is(err, ErrRegistrarUnavailable) 分支都不会命中。
// 服务名为空时返回 ErrServiceNameRequired（参数校验先于环境检查——空服务名
// 是调用方的错，报「registrar 不可用」会把它掩盖掉）。
func (c *ServiceClient) RegisterAs(ctx context.Context, serviceName string, port int, opts ...registry.RegisterOptionFunc) error {
	if serviceName == "" {
		return ErrServiceNameRequired
	}
	if c.registrar == nil {
		c.logRegisterNoOp(port)
		return nil
	}
	return c.registerAs(ctx, serviceName, port, opts...)
}

// registerAs 承载 Register/RegisterAs 的全部共同逻辑（#142）：buildInstance →
// provider 注册 → 快照缓存。调用方负责各自的 no-op 守卫与服务名校验。
func (c *ServiceClient) registerAs(ctx context.Context, serviceName string, port int, opts ...registry.RegisterOptionFunc) error {
	inst, err := c.buildInstance(serviceName, port, opts...)
	if err != nil {
		return err
	}
	if err := c.registrar.Register(ctx, inst); err != nil {
		return err
	}
	c.rememberRegistered(inst)
	return nil
}

// logRegisterNoOp 统一 Register/RegisterAs 的 no-op 日志（#142/#145）。
func (c *ServiceClient) logRegisterNoOp(port int) {
	// #66：「没注册器」（mock 模式/未启用注册发现）不是错误——幂等 no-op
	// success，与 nns.enabled:false 的「不注册」语义对齐（mock 开箱即用）；
	// 真故障（Nacos 连不上）走 registrar 内部 error 路径仍可区分。消费方
	// 不再需要自维护 Enabled&&!Mock 门控布尔并保证注册/注销对称。
	nnsLog.Info("no registrar configured (mock or discovery disabled), register is a no-op",
		"port", port)
}

// Deregister 从注册中心注销当前服务实例。
//
// 实例身份优先取 Register 成功时缓存的快照（#75）：注册中心按
// ip+port+service+group（+cluster）匹配实例，若注销时按当下环境重新推导——
// 注册与注销之间 GetPodIP 结果变化（网卡枚举顺序变化/POD_IP 变更）、或
// 注册时传了 WithGroup/WithCluster 而注销时漏传——就会匹配不到实例，
// 注销**静默失效**、实例残留在注册中心继续接流量。
//
// 没有缓存（本 client 未注册过，如经 Registrar() 逃生口自行注册的调用方）
// 时退回旧行为：按当下环境与 opts 现算实例身份——此时调用方必须保证
// IP/Group/Cluster 与注册时完全一致。注销成功后缓存清除，再次 Deregister
// 走现算路径。
//
// 服务名为空（未先 Init/LocalServiceInit 记录自身服务名）时返回
// ErrServiceNameRequired（#153，与 DeregisterAs 同序：参数校验先于环境
// 检查）——此前空名会以 Service="" 现算实例直接打给 provider，调用方
// 拿到的只是 provider 侧的晦涩错误，收不到「没先 Init」的根因信号。
func (c *ServiceClient) Deregister(ctx context.Context, port int, opts ...registry.RegisterOptionFunc) error {
	name := c.getSelfServiceName()
	if name == "" {
		return ErrServiceNameRequired
	}
	return c.deregisterAs(ctx, name, port, opts...)
}

// DeregisterAs 从注册中心注销指定服务名下的当前服务实例。
//
// 与 RegisterAs 配套：实例身份优先取 RegisterAs 成功时缓存的快照（#75），
// IP/Group/Cluster 不再依赖注销时的环境与 opts；无缓存时按当下环境现算，
// 调用方须自行保证与注册时一致。
//
// 未配置 registrar 时返回 nil（与 Deregister 一致：注销是幂等收尾，
// 没有 registrar 说明根本没注册过）；服务名为空时返回 ErrServiceNameRequired
// （参数校验先于环境检查）。
func (c *ServiceClient) DeregisterAs(ctx context.Context, serviceName string, port int, opts ...registry.RegisterOptionFunc) error {
	if serviceName == "" {
		return ErrServiceNameRequired
	}
	return c.deregisterAs(ctx, serviceName, port, opts...)
}

// deregisterAs 承载 Deregister/DeregisterAs 的全部共同逻辑（#142）：
// 快照优先（#75）→ 兜底现算 → provider 注销 → 清缓存。
func (c *ServiceClient) deregisterAs(ctx context.Context, serviceName string, port int, opts ...registry.RegisterOptionFunc) error {
	if c.registrar == nil {
		return nil
	}
	inst, ok := c.recallRegistered(serviceName, port)
	if !ok {
		var err error
		inst, err = c.buildInstance(serviceName, port, opts...)
		if err != nil {
			return err
		}
	}
	if err := c.registrar.Deregister(ctx, inst); err != nil {
		return err
	}
	c.forgetRegistered(serviceName, port)
	return nil
}

// instanceKey 是已注册实例缓存的键：服务名 + 端口唯一定位一次注册
// （同一服务多端口各注册一次，如 HTTP + RPC 双协议端点）。
type instanceKey struct {
	service string
	port    int
}

// rememberRegistered 缓存成功注册的实例身份（#75）。注册失败不缓存：
// 注册中心里没有对应实例，注销本就无从谈起。
func (c *ServiceClient) rememberRegistered(inst registry.Instance) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.registeredInstances == nil {
		c.registeredInstances = make(map[instanceKey]registry.Instance)
	}
	c.registeredInstances[instanceKey{service: inst.Service, port: inst.Port}] = inst
}

// recallRegistered 取回注册时缓存的实例身份；未注册过返回 ok=false。
func (c *ServiceClient) recallRegistered(serviceName string, port int) (registry.Instance, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	inst, ok := c.registeredInstances[instanceKey{service: serviceName, port: port}]
	return inst, ok
}

// forgetRegistered 在注销成功后清除缓存：再次 Deregister 回到现算路径
// （幂等语义与旧行为一致）。
func (c *ServiceClient) forgetRegistered(serviceName string, port int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.registeredInstances, instanceKey{service: serviceName, port: port})
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
