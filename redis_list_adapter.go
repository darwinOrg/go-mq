package dgmq

import (
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	redisdk "github.com/darwinOrg/go-redis"
	"github.com/google/uuid"
	"time"
)

type RedisListAdapter struct {
	RedisCli     redisdk.RedisCli
	Timeout      time.Duration
	closedTopics chan string
}

func NewRedisListAdapter(redisCli redisdk.RedisCli, timeout time.Duration) *RedisListAdapter {
	return &RedisListAdapter{
		RedisCli:     redisCli,
		Timeout:      timeout,
		closedTopics: make(chan string, 100),
	}
}

func (a *RedisListAdapter) Publish(ctx *dgctx.DgContext, topic string, message any) error {
	_, err := a.RedisCli.LPush(topic, message)
	if err != nil {
		dglogger.Errorf(ctx, "Publish error | topic: %s | err: %v", topic, err)
	}
	return err
}

func (a *RedisListAdapter) Destroy(ctx *dgctx.DgContext, topic string) error {
	a.closedTopics <- topic
	err := a.RedisCli.Del(topic)
	if err != nil {
		dglogger.Errorf(ctx, "Destroy error | topic: %s | err: %v", topic, err)

	}
	return err
}

func (a *RedisListAdapter) Subscribe(topic string, handler SubscribeHandler) error {
	go func() {
		for {
			select {
			case closedTopic := <-a.closedTopics:
				if closedTopic == topic {
					return
				}
			default:
				dc := &dgctx.DgContext{TraceId: uuid.NewString()}
				rts, readErr := a.RedisCli.BRPop(a.Timeout, topic)
				if readErr != nil {
					dglogger.Errorf(dc, "BRPop error | topic:%s | err:%v", topic, readErr)
					time.Sleep(time.Second)
					continue
				}
				if len(rts) == 2 {
					handlerErr := handler(dc, rts[1])
					if handlerErr != nil {
						dglogger.Errorf(dc, "Handle error | topic:%s | err:%v", topic, handlerErr)
						continue
					}
				}
			}
		}
	}()

	return nil
}

func (a *RedisListAdapter) Unsubscribe(ctx *dgctx.DgContext, topic string) error {
	return nil
}

func (a *RedisListAdapter) Acknowledge(ctx *dgctx.DgContext, topic string, messageId string) error {
	return nil
}
