package dgmq

import (
	"errors"
	dgctx "github.com/darwinOrg/go-common/context"
	redisdk "github.com/darwinOrg/go-redis"
	"time"
)

type SubscribeHandler func(ctx *dgctx.DgContext, message string) error

type MqAdapter interface {
	Publisher
	Subscriber
	Close()
}

type Publisher interface {
	Publish(ctx *dgctx.DgContext, topic string, message any) error
	Destroy(ctx *dgctx.DgContext, topic string) error
}

type Subscriber interface {
	Subscribe(topic string, handler SubscribeHandler) error
	DynamicSubscribe(closeCh chan struct{}, topic string, handler SubscribeHandler) error
}

const (
	MqAdapterRedisList   = "redis_list"
	MqAdapterRedisStream = "redis_stream"
	MqAdapterSmss        = "smss"
)

type MqAdapterConfig struct {
	Type      string        `json:"type" mapstructure:"type"`
	Host      string        `json:"host" mapstructure:"host"`
	Port      int           `json:"port" mapstructure:"port"`
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
