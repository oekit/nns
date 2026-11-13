package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/logx"
	"github.com/oekit/nns/metadata"
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
			nnsLog.Warn("get service config failed, using default", "service", serviceName, logx.ErrKey, err)
		case content != "":
			if err := json.Unmarshal([]byte(content), basic); err != nil {
				// 服务元数据（超时/重试/负载均衡调优项）解析失败只降级默认值，
				// 不拖垮整个 NNS 初始化：dataId 与应用 YAML 配置撞车、或元数据
				// 写坏时，服务发现整体不可用属于过重的惩罚。
				nnsLog.Warn("unmarshal service config failed, using default", "service", serviceName, logx.ErrKey, err)
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
//
// 在途可见性（#170）：去重标记与在途登记槽（watching）同帧发布——从标记
// 置位起整个注册期（含 ListenConfig 异步落地之前）对 UnwatchService/Close
// 可见，取消必须等注册出结果后再发（否则 dataId 粒度取消对未注册监听是
// no-op，监听随后落地成孤儿）。并发纪律：有在途退订（unwatching 在位）时
// 本方法先等退订出结果再登记——否则本方监听落地后可能被在途退订按 dataId
// 一并拆除，而本地标记已置位 ⇒ 永不重注册 ⇒ 热更静默失联（与 ensureSubscribe
// 等待 unsubscribing 同构）。
func (c *ServiceClient) WatchService(ctx context.Context, serviceName string) {
	if c.mockMode || c.configStore == nil {
		return
	}

	for {
		c.mu.Lock()
		// Close 后（终结语义）直接 no-op：不标记，否则残留去重标记
		if c.closed.Load() {
			c.mu.Unlock()
			return
		}
		// #170：有在途退订 ⇒ 等它出结果再登记（不持锁等待）。醒来后重新
		// 决策：退订可能已把标记摘除（本方重新登记）或已回滚（幂等重入）。
		// ctx 取消时直接返回——未登记、未注册，调用方按 WatchService 无
		// 返回值语义视作未监听。
		if ucall, ok := c.unwatching[serviceName]; ok {
			c.mu.Unlock()
			select {
			case <-ucall.done:
			case <-ctx.Done():
				return
			}
			continue
		}
		if c.watchedServices[serviceName] {
			c.mu.Unlock()
			return
		}
		if c.watchedServices == nil {
			c.watchedServices = make(map[string]bool)
		}
		c.watchedServices[serviceName] = true
		// #170：在途登记槽与去重标记同帧发布，注册全程对退订/Close 可见。
		call := &subscribeCall{done: make(chan struct{})}
		if c.watching == nil {
			c.watching = make(map[string]*subscribeCall)
		}
		c.watching[serviceName] = call
		c.mu.Unlock()

		async.GoSafe(func() {
			c.watchRegister(ctx, serviceName, call)
		})
		return
	}
}

// watchRegister 是 WatchService 后台注册 goroutine 的主体（#170 自
// WatchService 拆出）：ListenConfig 落地 → 失败回滚去重标记；成功后补偿
// 复查（closed **或已被 Unwatch**——标记被摘 ⇒ 取消先于本注册落地成了
// no-op，立即补一次 StopListenConfig 把竞态窗口收到「注册+补偿」内部）；
// 收场先摘在途槽再唤醒等待者（等待者看到「已出结果」终态再决策）。
func (c *ServiceClient) watchRegister(ctx context.Context, serviceName string, call *subscribeCall) {
	// 注册前复查 closed：Close 可能在去重标记之后、goroutine 执行之前
	// 完成快照——此时直接注册会泄漏监听器（无人再 Cancel）
	c.mu.RLock()
	closedBeforeRegister := c.closed.Load()
	c.mu.RUnlock()
	if closedBeforeRegister {
		c.finishWatching(serviceName, call)
		return
	}

	err := c.configStore.ListenConfig(ctx, serviceName, c.configGroup, func(data string) {
		nnsLog.Info("service config changed", "service", serviceName)

		basic := metadata.NewServiceBasic()
		if data != "" {
			if err := json.Unmarshal([]byte(data), basic); err != nil {
				nnsLog.Warn("unmarshal changed config failed", logx.ErrKey, err)
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
		callbacks := make([]*ConfigChangeHandle, len(c.configChangeCallbacks))
		copy(callbacks, c.configChangeCallbacks)
		c.mu.RUnlock()

		for _, cb := range callbacks {
			safeInvoke("config", serviceName, func() { cb.fn(serviceName) })
		}
	})
	if err != nil {
		// 回滚去重标记：允许下次 WatchService 重试，
		// 否则启动期一次瞬态失败会永久禁用该服务的配置热更新。
		// （在途槽在位期间 UnwatchService/并发 WatchService 都不会动标记
		// ——前者等待本槽，后者见标记在位走去重——无条件回滚安全。）
		c.mu.Lock()
		delete(c.watchedServices, serviceName)
		c.mu.Unlock()
		c.finishWatching(serviceName, call)
		nnsLog.Warn("listen service config failed", "service", serviceName, logx.ErrKey, err)
		return
	}

	// 注册成功后复查补偿条件（#170 扩展）：closed（Close 可能在注册期间
	// 跑完全部清理——当时 SDK 上还没有该 listener，StopListenConfig 是
	// no-op）**或已被 Unwatch**（在途期标记被摘，取消先于本注册落地同样
	// 是 no-op）——两种情况都立即补一次 Cancel，把竞态窗口收到
	// 「注册+补偿」内部。
	c.mu.RLock()
	compensate := c.closed.Load() || !c.watchedServices[serviceName]
	c.mu.RUnlock()
	if compensate {
		if stopErr := c.configStore.StopListenConfig(context.Background(), serviceName, c.configGroup); stopErr != nil {
			nnsLog.Warn("stop listen config after close failed", "service", serviceName, logx.ErrKey, stopErr)
		}
	}

	c.finishWatching(serviceName, call)
}

// finishWatching 摘除在途登记槽并唤醒等待者（#170）：先摘标记再 close(done)
// ——等待者醒来看到的是「注册已出结果、槽已收场」的终态。身份核验摘除
// （槽若已被并发 WatchService 重新登记则不动他方）。
func (c *ServiceClient) finishWatching(serviceName string, call *subscribeCall) {
	c.mu.Lock()
	if cur, ok := c.watching[serviceName]; ok && cur == call {
		delete(c.watching, serviceName)
	}
	c.mu.Unlock()
	close(call.done)
}

// UnwatchService 取消指定服务的配置监听（WatchService 的对称退订入口，
// #148）。
//
// 此前配置监听只能随 Close 全量终结：动态增删依赖服务的场景（网关按配置
// 热加载 dep 列表）里，下线服务的监听与 goroutine 只能积累到进程 Close。
// 本入口摘除去重标记并 StopListenConfig（ConfigStore 按 dataId 取消监听，
// Close 路径同款机制），单个服务的配置监听可独立回收。
//
// 未监听过的服务是幂等 no-op（返回 nil）；Mock 模式或未注入 ConfigStore
// 时同样 no-op（与 WatchService 的 no-op 对称）。取消后可重新 WatchService。
//
// 在途等待语义（#170）：监听注册是异步的（WatchService 刚返回时后台注册
// 可能尚未落地）——本方法先等在途注册出结果再取消（等价于把 dataId 粒度
// 取消确定地落在注册落地**之后**），保证返回时监听已回收；落地失败的注册
// （标记已回滚）按未监听幂等 no-op。并发多次 Unwatch 收敛：后来者等首次
// 退订出结果后按「未监听」重入（对 provider 幂等）。
func (c *ServiceClient) UnwatchService(ctx context.Context, serviceName string) error {
	if c.mockMode || c.configStore == nil {
		return nil
	}

	for {
		c.mu.Lock()
		// #170：等在途注册落地——StopListenConfig 先于 ListenConfig 落地
		// 是 no-op，监听随后落地成孤儿：再次 Unwatch 因标记已摘而 no-op、
		// Close 的取消清单来自快照同样不含它。等待后按落地结果决策。
		if call, ok := c.watching[serviceName]; ok {
			c.mu.Unlock()
			select {
			case <-call.done:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		// 已有在途退订 ⇒ 等它出结果（并发退订去重到单个 provider 调用）。
		if ucall, ok := c.unwatching[serviceName]; ok {
			c.mu.Unlock()
			select {
			case <-ucall.done:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		if !c.watchedServices[serviceName] {
			c.mu.Unlock()
			return nil
		}
		ucall := &subscribeCall{done: make(chan struct{})}
		if c.unwatching == nil {
			c.unwatching = make(map[string]*subscribeCall)
		}
		c.unwatching[serviceName] = ucall
		delete(c.watchedServices, serviceName)
		c.mu.Unlock()

		err := c.configStore.StopListenConfig(ctx, serviceName, c.configGroup)

		// 先摘标记再唤醒等待者：并发 WatchService 醒来后按「退订已出结果、
		// 本地干净」的终态重新决策（与 unsubscribing 收场次序同口径）。
		c.mu.Lock()
		if cur, ok := c.unwatching[serviceName]; ok && cur == ucall {
			delete(c.unwatching, serviceName)
		}
		c.mu.Unlock()
		close(ucall.done)

		if err != nil {
			// 失败回滚去重标记：与 WatchService 注册失败的回滚同口径，
			// 允许重试 UnwatchService。不覆盖他人的新标记——唤醒的并发
			// WatchService 可能在回滚前已重新登记（标记在位即不动）。
			c.mu.Lock()
			if !c.watchedServices[serviceName] {
				c.watchedServices[serviceName] = true
			}
			c.mu.Unlock()
			return fmt.Errorf("stop listen config [%s]: %w", serviceName, err)
		}
		return nil
	}
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

// OnConfigChange 注册配置变更回调，返回注销句柄。
// 当 WatchService 检测到服务配置变更时，所有已注册的回调会被触发；
// 回调列表在事件分发时现读，注册顺序不影响事件送达。
// 典型用途：endpoint factory 注册 balancer 缓存重置。
//
// 保存返回的句柄并在组件 Close 时传给 OffConfigChange 注销——长生命周期的
// ServiceClient 会经回调闭包持有注册方，注册方废弃后不注销则无法回收，
// 配置事件也会继续打在死组件上（oekit/nns-endpoint#135）。
func (c *ServiceClient) OnConfigChange(fn func(serviceName string)) *ConfigChangeHandle {
	entry := &ConfigChangeHandle{fn: fn}
	c.mu.Lock()
	c.configChangeCallbacks = append(c.configChangeCallbacks, entry)
	c.mu.Unlock()
	return entry
}

// OffConfigChange 按 OnConfigChange 返回的句柄注销对应回调。
// 幂等：nil 句柄、重复注销或句柄已随 Close 清空均为 no-op。
func (c *ServiceClient) OffConfigChange(handle *ConfigChangeHandle) {
	if handle == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, cb := range c.configChangeCallbacks {
		if cb == handle {
			c.configChangeCallbacks = append(c.configChangeCallbacks[:i], c.configChangeCallbacks[i+1:]...)
			return
		}
	}
}
