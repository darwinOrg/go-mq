package dgmq

import (
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	redisdk "github.com/darwinOrg/go-redis"
	"github.com/google/uuid"
	"time"
)

type redisListAdapter struct {
	redisCli     redisdk.RedisCli
	timeout      time.Duration
	closedTopics chan string
}

func NewRedisListAdapter(redisCli redisdk.RedisCli, timeout time.Duration) MqAdapter {
	return &redisListAdapter{
		redisCli:     redisCli,
		timeout:      timeout,
		closedTopics: make(chan string, 100),
	}
}

func (a *redisListAdapter) Publish(ctx *dgctx.DgContext, topic string, message any) error {
	_, err := a.redisCli.LPush(topic, message)
	if err != nil {
		dglogger.Errorf(ctx, "Publish error | topic: %s | err: %v", topic, err)
	}
	return err
}

func (a *redisListAdapter) Destroy(ctx *dgctx.DgContext, topic string) error {
	a.closedTopics <- topic
	err := a.redisCli.Del(topic)
	if err != nil {
		dglogger.Errorf(ctx, "Destroy error | topic: %s | err: %v", topic, err)
	}
	return err
}

func (a *redisListAdapter) Subscribe(topic string, handler SubscribeHandler) error {
	go func() {
		for {
			select {
			case closedTopic := <-a.closedTopics:
				if closedTopic == topic {
					return
				}
			default:
				dc := &dgctx.DgContext{TraceId: uuid.NewString()}
				rts, readErr := a.redisCli.BRPop(a.timeout, topic)
				if readErr != nil {
					dglogger.Errorf(dc, "BRPop error | topic:%s | err:%v", topic, readErr)
					time.Sleep(time.Second)
					continue
				}
				if len(rts) == 2 {
					handlerErr := handler(dc, rts[1])
					if handlerErr != nil {
						dglogger.Errorf(dc, "Handle error | topic:%s | err:%v", topic, handlerErr)
					}
				}
			}
		}
	}()

	return nil
}

func (a *redisListAdapter) Unsubscribe(ctx *dgctx.DgContext, topic string) error {
	return nil
}

func (a *redisListAdapter) Acknowledge(ctx *dgctx.DgContext, topic string, messageId string) error {
	return nil
}
