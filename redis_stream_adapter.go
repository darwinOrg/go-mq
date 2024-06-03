package dgmq

import (
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	redisdk "github.com/darwinOrg/go-redis"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"sync"
	"time"
)

type RedisStreamAdapter struct {
	RedisCli     redisdk.RedisCli
	Group        string
	Consumer     string
	Block        time.Duration
	Count        int64
	closedTopics sync.Map
}

func (a *RedisStreamAdapter) Publish(ctx *dgctx.DgContext, topic string, message any) error {
	_, err := a.RedisCli.XAdd(&redis.XAddArgs{
		Stream: topic,
		Values: message,
	})
	if err != nil {
		dglogger.Errorf(ctx, "publish message error, topic: %s, err: %v", topic, err)
		return err
	}

	return nil
}

func (a *RedisStreamAdapter) Destroy(ctx *dgctx.DgContext, topic string) error {
	a.closedTopics.Store(topic, true)

	err := a.RedisCli.Del(topic)
	if err != nil {
		dglogger.Errorf(ctx, "destroy topic error, topic: %s, err: %v", topic, err)
		return err
	}

	return nil
}

func (a *RedisStreamAdapter) Subscribe(topic string, handler SubscribeHandler) error {
	_, err := a.RedisCli.XGroupCreateMkStream(topic, a.Group, "$")
	if err != nil {
		return err
	}

	go func() {
		for {
			_, ok := a.closedTopics.Load(topic)
			if ok {
				break
			}

			ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
			xstreams, readErr := a.RedisCli.XReadGroup(&redis.XReadGroupArgs{
				Group:    a.Group,
				Consumer: a.Consumer,
				Streams:  []string{topic, ">"},
				Count:    a.Count,
				Block:    a.Block,
			})
			if readErr != nil {
				_, ok := a.closedTopics.Load(topic)
				if !ok {
					dglogger.Errorf(ctx, "XREADGROUP 错误 |topic:%s | err:%v", topic, readErr)
				}
				time.Sleep(time.Second)
				continue
			}

			for _, xstream := range xstreams {
				for _, xmessage := range xstream.Messages {
					handlerErr := handler(ctx, xmessage.Values)

					if handlerErr != nil {
						dglogger.Errorf(ctx, "Accept Message 错误 |topic:%s | err:%v", topic, handlerErr)
						continue
					}

					_ = a.Acknowledge(ctx, topic, xmessage.ID)
				}
			}
		}
	}()

	return nil
}

func (a *RedisStreamAdapter) Unsubscribe(topic string) error {
	a.closedTopics.Store(topic, true)
	return nil
}

func (a *RedisStreamAdapter) Acknowledge(ctx *dgctx.DgContext, topic string, messageId string) error {
	_, ackErr := a.RedisCli.XAck(topic, a.Group, messageId)
	if ackErr != nil {
		dglogger.Errorf(ctx, "Accept Message Ack 错误 |topic:%s | err:%v", topic, ackErr)
	}
	return ackErr
}
