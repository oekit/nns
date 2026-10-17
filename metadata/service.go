package metadata

import (
	"encoding/json"
	"regexp"
	"strings"
	"time"
)

// ServiceName 服务全名，包含区域、业务线和协议等维度信息。
type ServiceName struct {
	FullName string `json:"full_name"`
	Region   string `json:"region"`
	Biz1     string `json:"biz1"`
	Biz2     string `json:"biz2"`
	Name     string `json:"name"`
	Protocol string `json:"protocol"`
}

// ParseServiceName 将 "region.biz1.biz2.name.protocol" 形式的全名解析为 ServiceName。
// 段数不为 5 时返回仅填充 FullName 的 ServiceName（兼容未规范化的服务名）。
// 空串返回 nil。
func ParseServiceName(fullName string) *ServiceName {
	if fullName == "" {
		return nil
	}
	parts := strings.Split(fullName, ".")
	if len(parts) != 5 {
		return &ServiceName{FullName: fullName}
	}
	return &ServiceName{
		FullName: fullName,
		Region:   parts[0],
		Biz1:     parts[1],
		Biz2:     parts[2],
		Name:     parts[3],
		Protocol: parts[4],
	}
}

// ConnectionConfig 连接配置（超时、重试、负载均衡策略）。
type ConnectionConfig struct {
	ConnTimeout  int    `json:"conn_timeout"`  // 连接超时（毫秒）
	ReadTimeout  int    `json:"read_timeout"`  // 读超时（毫秒）
	WriteTimeout int    `json:"write_timeout"` // 写超时（毫秒）
	Retry        int    `json:"retry"`         // 重试次数
	Lb           string `json:"lb"`            // 负载均衡策略
}

// ACLConfig 访问控制配置。
type ACLConfig struct {
	Acl            bool                              `json:"acl"`
	AclPolicy      string                            `json:"acl_policy"`
	AclRule        map[string]map[string]int         `json:"acl_rule"`
	AclRulePattern map[*regexp.Regexp]map[string]int `json:"-"`
}

// ServiceBasic 服务基本配置，包含超时、重试、负载均衡策略和鉴权等信息。
//
// 使用嵌入子结构体按职责分组，字段访问方式不变（Go 自动提升）：
//
//	sb.ConnTimeout        // 等价于 sb.ConnectionConfig.ConnTimeout
//	sb.Auth.Redis.Token   // 等价于 sb.AuthConfig.Auth.Redis.Token
type ServiceBasic struct {
	ConnectionConfig                 // 连接配置
	Auth             Auth            `json:"auth"`
	ACLConfig                        // 访问控制配置
	Route            Route           `json:"route"`
	Extra            any             `json:"extra"`
	Endpoints        any             `json:"endpoints"`
	TimeoutRetryCfg  []*TimeoutRetry `json:"timeout_retry_rule"`
	ServiceName      ServiceName     `json:"service_name"`
}

// TimeoutRetry 按接口维度的超时重试配置。
type TimeoutRetry struct {
	Api     string `json:"api"`
	Type    int    `json:"type"`
	Timeout int    `json:"timeout"`
	Retry   int    `json:"retry"`
	Enable  bool   `json:"enable"`
}

// Auth 各类中间件和服务的统一鉴权配置。
type Auth struct {
	MySQL         MySQLAuth         `json:"mysql"`
	Redis         RedisAuth         `json:"redis"`
	HTTP          HTTPAuth          `json:"http"`
	RocketMQ      RocketMQAuth      `json:"rocket_mq"`
	Kafka         KafkaAuth         `json:"kafka"`
	Elasticsearch ElasticsearchAuth `json:"elasticsearch"`
}

// RedisAuth Redis 鉴权配置。
type RedisAuth struct {
	Token string `json:"token"`
	User  string `json:"user"`
}

// MySQLAuth MySQL 鉴权配置。
type MySQLAuth struct {
	DBName string  `json:"dbname"`
	Master UserPsw `json:"master"`
	Slave  UserPsw `json:"slave"`
}

// UserPsw 用户名密码对。
type UserPsw struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// HTTPAuth HTTP 服务鉴权配置。
type HTTPAuth struct {
	Secret string `json:"secret"`
}

// RocketMQAuth RocketMQ 鉴权配置。
type RocketMQAuth struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

// KafkaAuth Kafka 鉴权配置。
type KafkaAuth struct{}

// ElasticsearchAuth Elasticsearch 鉴权配置。
type ElasticsearchAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Route 服务路由规则定义。
type Route []map[string]map[string]string

// Endpoint 服务端点信息，包含地址、端口和元数据。
type Endpoint struct {
	Addr string            `json:"addr"`
	Port int               `json:"port"`
	Meta map[string]string `json:"meta"`
}

// NewServiceBasic 创建带有默认值的 ServiceBasic 实例。
func NewServiceBasic() *ServiceBasic {
	return &ServiceBasic{
		ConnectionConfig: ConnectionConfig{
			ConnTimeout:  1000,
			ReadTimeout:  5000,
			WriteTimeout: 5000,
			Retry:        3,
			Lb:           LbRoundRobin,
		},
	}
}

// GetEndpoints 将 Endpoints 字段解析为 Endpoint 列表。
func (sb *ServiceBasic) GetEndpoints() []Endpoint {
	var eps []Endpoint
	if sb.Endpoints == nil {
		return eps
	}

	data, err := json.Marshal(sb.Endpoints)
	if err != nil {
		return eps
	}

	if err := json.Unmarshal(data, &eps); err != nil {
		return eps
	}
	return eps
}

// GetTimeoutRetryConfig 根据协议、服务名和接口名获取超时重试配置。
func (sb *ServiceBasic) GetTimeoutRetryConfig(protocol, serviceName, api string) (timeout int, retry int, hit bool) {
	if sb == nil {
		return
	}

	for _, item := range sb.TimeoutRetryCfg {
		if item == nil || !item.Enable {
			continue
		}

		if item.Api == api {
			return item.Timeout, item.Retry, true
		}
	}

	return 0, 0, false
}

// GRPCOption gRPC 调用选项。
type GRPCOption struct {
	ServiceName     string
	GrpcServiceName string
	Method          string
	Timeout         time.Duration
	Retry           int
}
