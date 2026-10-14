package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yi-nology/ekit-core/concurrency/async"
	"github.com/yi-nology/ekit-nns/nns/metadata"
)

// LocalServiceInit 初始化本地服务，从 Nacos 配置中心拉取服务配置。
func (c *ServiceClient) LocalServiceInit(ctx context.Context, serviceName string) (*metadata.ServiceBasic, error) {
	nnsLog.Info("initializing local service", "service", serviceName)

	// Mock 模式或有本地配置
	if c.mockMode {
		c.mu.Lock()
		c.selfServiceName = serviceName
		if basic, ok := c.localServiceConfig[serviceName]; ok {
			c.localService = basic
			c.mu.Unlock()
			return basic, nil
		}
		basic := metadata.NewServiceBasic()
		c.localService = basic
		c.mu.Unlock()
		return basic, nil
	}

	// 网络 IO（GetConfig，SDK 默认 5s 超时）在锁外完成：c.mu 同时保护请求
	// 路径的 GetServiceBasic，持锁跨 IO 会让已就绪服务的请求被未就绪配置
	// 卡住（balancerCache 同类问题已修过，这里是漏网的两处之一）。
	// 历史数据竞争的根源是"锁内发布 c.localService 后继续改共享对象"，
	// 改为本地副本构建完再锁内一次性发布，与 WatchService 回调无竞争。
	basic := metadata.NewServiceBasic()

	if c.configStore != nil {
		content, err := c.configStore.GetConfig(ctx, serviceName, c.configGroup)
		switch {
		case err != nil:
			nnsLog.Warn("get service config failed, using default", "service", serviceName, "err", err)
		case content != "":
			if err := json.Unmarshal([]byte(content), basic); err != nil {
				return nil, fmt.Errorf("unmarshal service config [%s]: %w", serviceName, err)
			}
		}
	} else {
		nnsLog.Warn("config store not available, using default config")
	}

	c.BindServiceBasic(serviceName, basic)

	c.mu.Lock()
	c.selfServiceName = serviceName
	c.localService = basic
	c.mu.Unlock()

	nnsLog.Info("local service initialized", "service", basic)
	return basic, nil
}

// LoadDepService 加载依赖服务的配置信息。
// 采用尽力而为策略：即使某个服务加载失败，也会继续加载其余服务。
// 所有遇到的错误将通过 errors.Join 聚合后返回。
func (c *ServiceClient) LoadDepService(ctx context.Context, serviceNames []string) error {
	// GetConfig 网络 IO 在锁外逐服务完成（本地 map 暂存结果），最后持锁
	// 一次性提交——此前整个循环持写锁，任一服务配置慢都会卡住请求路径的
	// GetServiceBasic 与 WatchService 回调。
	var errs []error
	pending := make(map[string]*metadata.ServiceBasic, len(serviceNames))

	for _, sn := range serviceNames {
		dep := metadata.NewServiceBasic()

		// Mock 模式或有本地配置（localServiceConfig 仅构造期写入，只读安全）
		if c.mockMode {
			if basic, ok := c.localServiceConfig[sn]; ok {
				dep = basic
			}
			pending[sn] = dep
			continue
		}

		if c.configStore == nil {
			nnsLog.Warn("config store not available, using default", "service", sn)
			pending[sn] = dep
			continue
		}

		content, err := c.configStore.GetConfig(ctx, sn, c.configGroup)
		if err != nil {
			errs = append(errs, fmt.Errorf("get config [%s]: %w", sn, err))
			pending[sn] = dep
			continue
		}

		if content != "" {
			if err := json.Unmarshal([]byte(content), dep); err != nil {
				errs = append(errs, fmt.Errorf("unmarshal config [%s]: %w", sn, err))
			}
		}

		pending[sn] = dep
		nnsLog.Info("dep service loaded", "service", sn)
	}

	// 持锁一次性提交（BindServiceBasic 原地改 basic，必须与发布同帧）
	c.mu.Lock()
	for sn, dep := range pending {
		c.BindServiceBasic(sn, dep)
		c.depService[sn] = dep
	}
	c.mu.Unlock()

	return errors.Join(errs...)
}

// WatchService 监听指定服务的配置变更。
// 内部做去重：同一 serviceName 只注册一次监听。
func (c *ServiceClient) WatchService(ctx context.Context, serviceName string) {
	if c.mockMode || c.configStore == nil {
		return
	}

	c.mu.Lock()
	// Close 后（终结语义）直接 no-op：不标记，否则残留去重标记
	if c.closed {
		c.mu.Unlock()
		return
	}
	if c.watchedServices[serviceName] {
		c.mu.Unlock()
		return
	}
	if c.watchedServices == nil {
		c.watchedServices = make(map[string]bool)
	}
	c.watchedServices[serviceName] = true
	c.mu.Unlock()

	async.GoSafe(func() {
		// 注册前复查 closed：Close 可能在去重标记之后、goroutine 执行之前
		// 完成快照——此时直接注册会泄漏监听器（无人再 Cancel）
		c.mu.RLock()
		if c.closed {
			c.mu.RUnlock()
			return
		}
		c.mu.RUnlock()

		err := c.configStore.ListenConfig(ctx, serviceName, c.configGroup, func(data string) {
			nnsLog.Info("service config changed", "service", serviceName)

			basic := metadata.NewServiceBasic()
			if data != "" {
				if err := json.Unmarshal([]byte(data), basic); err != nil {
					nnsLog.Warn("unmarshal changed config failed", "err", err)
					return
				}
			}

			c.BindServiceBasic(serviceName, basic)

			c.mu.Lock()
			if c.localService != nil && serviceName == c.localService.ServiceName.FullName {
				c.localService = basic
			} else if dep, ok := c.depService[serviceName]; ok {
				*dep = *basic
			}
			c.mu.Unlock()

			// 触发配置变更回调：分发时现读回调列表（而非注册时快照），
			// 保证 WatchService 之后注册的回调同样收到事件
			c.mu.RLock()
			callbacks := make([]func(string), len(c.configChangeCallbacks))
			copy(callbacks, c.configChangeCallbacks)
			c.mu.RUnlock()

			for _, fn := range callbacks {
				safeInvoke("config", serviceName, func() { fn(serviceName) })
			}
		})
		if err != nil {
			// 回滚去重标记：允许下次 WatchService 重试，
			// 否则启动期一次瞬态失败会永久禁用该服务的配置热更新
			c.mu.Lock()
			delete(c.watchedServices, serviceName)
			c.mu.Unlock()
			nnsLog.Warn("listen service config failed", "service", serviceName, "err", err)
			return
		}

		// 注册成功后复查 closed：Close 可能在注册期间跑完全部清理
		// （当时 SDK 上还没有该 listener，StopListenConfig 是 no-op）——
		// 立即补一次 Cancel，把竞态窗口收到"注册+补偿"内部
		c.mu.RLock()
		closedDuringRegister := c.closed
		c.mu.RUnlock()
		if closedDuringRegister {
			if stopErr := c.configStore.StopListenConfig(context.Background(), serviceName, c.configGroup); stopErr != nil {
				nnsLog.Warn("stop listen config after close failed", "service", serviceName, "err", stopErr)
			}
		}
	})
}

// safeInvoke 带 panic 隔离地执行用户回调：
// 这些回调运行在 provider SDK 的回调 goroutine 内，用户回调 panic 不允许击穿进程。
func safeInvoke(kind, serviceName string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			nnsLog.Error("nns callback panicked", "kind", kind, "service", serviceName, "panic", r)
		}
	}()
	fn()
}

// OnConfigChange 注册配置变更回调。
// 当 WatchService 检测到服务配置变更时，所有已注册的回调会被触发；
// 回调列表在事件分发时现读，注册顺序不影响事件送达。
// 典型用途：endpoint factory 注册 balancer 缓存重置。
func (c *ServiceClient) OnConfigChange(fn func(serviceName string)) {
	c.mu.Lock()
	c.configChangeCallbacks = append(c.configChangeCallbacks, fn)
	c.mu.Unlock()
}
