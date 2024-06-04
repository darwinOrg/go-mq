package dgmq

import (
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	redisdk "github.com/darwinOrg/go-redis"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"time"
)

type RedisStreamAdapter struct {
	RedisCli     redisdk.RedisCli
	Group        string
	Consumer     string
	Block        time.Duration
	Count        int64
	closedTopics chan string
}

func NewRedisStreamAdapter(redisCli redisdk.RedisCli, group string, consumer string, block time.Duration, count int64) *RedisStreamAdapter {
	return &RedisStreamAdapter{
		RedisCli:     redisCli,
		Group:        group,
		Consumer:     consumer,
		Block:        block,
		Count:        count,
		closedTopics: make(chan string, 100),
	}
}

func (a *RedisStreamAdapter) Publish(ctx *dgctx.DgContext, topic string, message any) error {
	_, err := a.RedisCli.XAdd(&redis.XAddArgs{
		Stream: topic,
		Values: message,
	})
	if err != nil {
		dglogger.Errorf(ctx, "XAdd error | topic: %s | err: %v", topic, err)
	}
	return err
}

func (a *RedisStreamAdapter) Destroy(ctx *dgctx.DgContext, topic string) error {
	a.closedTopics <- topic
	err := a.RedisCli.Del(topic)
	if err != nil {
		dglogger.Errorf(ctx, "Destroy error | topic: %s | err: %v", topic, err)
	}
	return err
}

func (a *RedisStreamAdapter) Subscribe(topic string, handler SubscribeHandler) error {
	_, err := a.RedisCli.XGroupCreateMkStream(topic, a.Group, "$")
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case closedTopic := <-a.closedTopics:
				if closedTopic == topic {
					return
				}
			default:
				dc := &dgctx.DgContext{TraceId: uuid.NewString()}
				xstreams, readErr := a.RedisCli.XReadGroup(&redis.XReadGroupArgs{
					Group:    a.Group,
					Consumer: a.Consumer,
					Streams:  []string{topic, ">"},
					Count:    a.Count,
					Block:    a.Block,
				})
				if readErr != nil {
					dglogger.Errorf(dc, "XReadGroup error | topic:%s | err:%v", topic, readErr)
					time.Sleep(time.Second)
					continue
				}

				for _, xstream := range xstreams {
					for _, xmessage := range xstream.Messages {
						handlerErr := handler(dc, xmessage.Values)
						if handlerErr != nil {
							dglogger.Errorf(dc, "Handle error | topic:%s | err:%v", topic, handlerErr)
							continue
						}

						_ = a.Acknowledge(dc, topic, xmessage.ID)
					}
				}
			}
		}
	}()

	return nil
}

func (a *RedisStreamAdapter) Unsubscribe(ctx *dgctx.DgContext, topic string) error {
	a.closedTopics <- topic
	_, err := a.RedisCli.XGroupDestroy(topic, a.Group)
	if err != nil {
		dglogger.Errorf(ctx, "XGroupDestroy error |topic:%s | err:%v", topic, err)
	}
	return err
}

func (a *RedisStreamAdapter) Acknowledge(ctx *dgctx.DgContext, topic string, messageId string) error {
	_, err := a.RedisCli.XAck(topic, a.Group, messageId)
	if err != nil {
		dglogger.Errorf(ctx, "Acknowledge error |topic:%s | err:%v", topic, err)
	}
	return err
}
