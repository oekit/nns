// Package metadata 定义了 NNS（enjoydream Naming Service）中使用的核心元数据类型和常量。
//
// 包含服务名称解析结构 ServiceName（格式为 region.biz1.biz2.name.protocol）、
// 服务基础配置 ServiceBasic（超时、重试、负载均衡策略、认证、路由等）、
// 以及各类基础设施的认证信息（MySQL、Redis、HTTP、RocketMQ、Kafka、Elasticsearch）。
// 同时定义了协议类型常量、负载均衡策略常量和 NNS HTTP 头部常量，供整个 NNS 体系共享使用。
package metadata
