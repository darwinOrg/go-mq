package dgmq

import (
	"errors"
	dgctx "github.com/darwinOrg/go-common/context"
	redisdk "github.com/darwinOrg/go-redis"
	"time"
)

const (
	RequestIdHeader = "request_id"
)

type SubscribeHandler func(ctx *dgctx.DgContext, message string) error
type SubscribeEndCallback func()

type MqAdapter interface {
	Publisher
	Subscriber
}

type Publisher interface {
	CreateTopic(ctx *dgctx.DgContext, topic string) error
	Publish(ctx *dgctx.DgContext, topic string, message any) error
	PublishWithTag(ctx *dgctx.DgContext, topic, tag string, message any) error
	Destroy(ctx *dgctx.DgContext, topic string) error
	Close()
}

type Subscriber interface {
	Subscribe(ctx *dgctx.DgContext, topic string, handler SubscribeHandler) (SubscribeEndCallback, error)
	SubscribeWithTag(ctx *dgctx.DgContext, topic, tag string, handler SubscribeHandler) (SubscribeEndCallback, error)
	DynamicSubscribe(ctx *dgctx.DgContext, closeCh chan struct{}, topic string, handler SubscribeHandler) error
	CleanTag(ctx *dgctx.DgContext, topic, tag string) error
}

const (
	MqAdapterRedisList   = "redis_list"
	MqAdapterRedisStream = "redis_stream"
	MqAdapterNats        = "nats"
	MqAdapterSmss        = "smss"
)

type MqAdapterConfig struct {
	Type      string        `json:"type" mapstructure:"type"`
	Host      string        `json:"host" mapstructure:"host"`
	Port      int           `json:"port" mapstructure:"port"`
	Username  string        `json:"username" mapstructure:"username"`
	Password  string        `json:"password" mapstructure:"password"`
	Timeout   time.Duration `json:"timeout" mapstructure:"timeout"`
	PoolSize  int           `json:"poolSize" mapstructure:"poolSize"`
	Group     string        `json:"group" mapstructure:"group"`
	Consumer  string        `json:"consumer" mapstructure:"consumer"`
	BatchSize int64         `json:"batchSize" mapstructure:"batchSize"`
}

func NewMqAdapter(config *MqAdapterConfig) (MqAdapter, error) {
	var mqAdapter MqAdapter
	switch config.Type {
	case MqAdapterRedisList:
		mqAdapter = NewRedisListAdapter(redisdk.GetDefaultRedisCli(), config)
	case MqAdapterRedisStream:
		mqAdapter = NewRedisStreamAdapter(redisdk.GetDefaultRedisCli(), config)
	case MqAdapterNats:
		mqAdapter = NewNatsAdapter(redisdk.GetDefaultRedisCli(), config)
	case MqAdapterSmss:
		var err error
		mqAdapter, err = NewSmssAdapter(redisdk.GetDefaultRedisCli(), config)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("错误的类型")
	}

	return mqAdapter, nil
}
