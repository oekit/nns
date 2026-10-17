package metadata

// 协议类型常量
const (
	// ProtocolHTTP HTTP 协议。
	ProtocolHTTP = "http"
	// ProtocolGRPC gRPC 协议。
	ProtocolGRPC = "grpc"
	// ProtocolRedis Redis 协议。
	ProtocolRedis = "redis"
	// ProtocolMySQL MySQL 协议。
	ProtocolMySQL = "mysql"
	// ProtocolRocketMQ RocketMQ 协议。
	ProtocolRocketMQ = "rocketmq"
	// ProtocolKafka Kafka 协议。
	ProtocolKafka = "kafka"
	// ProtocolElasticsearch Elasticsearch 协议。
	ProtocolElasticsearch = "elasticsearch"

	// LbRoundRobin 轮询负载均衡策略。
	LbRoundRobin = "roundrobin"
	// LbRandom 随机负载均衡策略。
	LbRandom = "random"
	// LbWeightedRandom 加权随机负载均衡策略。
	LbWeightedRandom = "weighted"

	// DefaultHTTPPort 默认 HTTP 端口。
	DefaultHTTPPort = 8080
	// DefaultGRPCPort 默认 gRPC 端口。
	DefaultGRPCPort = 9090

	// HeaderColor 灰度颜色标识请求头。
	HeaderColor = "x-nns-color"
	// HeaderRegion 区域标识请求头。
	HeaderRegion = "x-nns-region"
	// HeaderServiceName 服务名标识请求头。
	HeaderServiceName = "x-nns-servicename"
)
