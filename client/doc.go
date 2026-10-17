// Package client 提供 NNS（enjoydream Naming Service）治理核心：
// 服务发现、配置管理、实例订阅与注册的 provider 无关实现。
//
// ServiceClient 不耦合任何注册中心 SDK：Registrar/Discoverer/Subscriber/
// ConfigStore 经 With* 选项注入，未注入时处于降级态（mock/静态实例/缓存兜底）。
// Nacos 默认构造由 nns 根门面（nns.New）承担——门面是 provider 选择点。
// 每次调用 New 返回独立实例（v0.15.2 起无包级单例）。
//
// 能力一览：
//   - 实例发现带五级降级链（静态 → mock → 新鲜缓存 → singleflight
//     Discover → stale 缓存兜底），provider 抖动不断服务
//   - 配置热更新（WatchService / OnConfigChange），超时/重试/LB 策略
//     等运行时生效
//   - Close 为终结操作：注销监听与订阅、关闭默认路径创建的 SDK 客户端，
//     幂等；复用需重新 New
//
// 该包被 nns/kitex、nns/hertz、nns/endpoint 适配层消费，将完整降级链
// 桥接到各框架的服务发现接口。
package client
