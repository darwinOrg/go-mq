package dgmq

import (
	dgctx "github.com/darwinOrg/go-common/context"
	"github.com/darwinOrg/go-common/utils"
	dglogger "github.com/darwinOrg/go-logger"
	redisdk "github.com/darwinOrg/go-redis"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"time"
)

const (
	defaultRedisStreamKey = "###redis_stream###"
)

type redisStreamAdapter struct {
	redisCli redisdk.RedisCli
	group    string
	consumer string
	block    time.Duration
	count    int64
}

func NewRedisStreamAdapter(redisCli redisdk.RedisCli, group string, consumer string, block time.Duration, count int64) MqAdapter {
	return &redisStreamAdapter{
		redisCli: redisCli,
		group:    group,
		consumer: consumer,
		block:    block,
		count:    count,
	}
}

func (a *redisStreamAdapter) Publish(ctx *dgctx.DgContext, topic string, message any) error {
	var values map[string]any
	switch message.(type) {
	case string:
		values = map[string]any{defaultRedisStreamKey: message}
	case []byte:
		values = map[string]any{defaultRedisStreamKey: string(message.([]byte))}
	default:
		jsonMsg, err := utils.ConvertBeanToJsonString(message)
		if err != nil {
			dglogger.Errorf(ctx, "ConvertBeanToJsonString error | topic: %s | err: %v", topic, err)
			return err
		}

		values = map[string]any{defaultRedisStreamKey: jsonMsg}
	}

	_, err := a.redisCli.XAdd(&redis.XAddArgs{
		Stream: topic,
		Values: values,
	})
	if err != nil {
		dglogger.Errorf(ctx, "XAdd error | topic: %s | err: %v", topic, err)
	}
	return err
}

func (a *redisStreamAdapter) Destroy(ctx *dgctx.DgContext, topic string) error {
	err := a.redisCli.Del(topic)
	if err != nil {
		dglogger.Errorf(ctx, "Destroy error | topic: %s | err: %v", topic, err)
	}
	return err
}

func (a *redisStreamAdapter) Subscribe(topic string, handler SubscribeHandler) error {
	_, err := a.redisCli.XGroupCreateMkStream(topic, a.group, "$")
	if err != nil {
		return err
	}

	go func() {
		for {
			a.subscribe(topic, handler)
		}
	}()

	return nil
}

func (a *redisStreamAdapter) DynamicSubscribe(closeCh chan struct{}, topic string, handler SubscribeHandler) error {
	_, err := a.redisCli.XGroupCreateMkStream(topic, a.group, "$")
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-closeCh:
				dc := &dgctx.DgContext{TraceId: uuid.NewString()}
				dglogger.Infof(dc, "closed topic: %s ", topic)
				return
			default:
				a.subscribe(topic, handler)
			}
		}
	}()

	return nil
}

func (a *redisStreamAdapter) subscribe(topic string, handler SubscribeHandler) {
	dc := &dgctx.DgContext{TraceId: uuid.NewString()}
	xstreams, readErr := a.redisCli.XReadGroup(&redis.XReadGroupArgs{
		Group:    a.group,
		Consumer: a.consumer,
		Streams:  []string{topic, ">"},
		Count:    a.count,
		Block:    a.block,
	})
	if readErr != nil {
		dglogger.Errorf(dc, "XReadGroup error | topic:%s | err:%v", topic, readErr)
		time.Sleep(time.Second)
		return
	}

	for _, xstream := range xstreams {
		for _, xmessage := range xstream.Messages {
			message := xmessage.Values[defaultRedisStreamKey].(string)
			handlerErr := handler(dc, message)
			if handlerErr != nil {
				dglogger.Errorf(dc, "Handle error | topic:%s | err:%v", topic, handlerErr)
				continue
			}

			_ = a.Acknowledge(dc, topic, xmessage.ID)
		}
	}

	return
}

func (a *redisStreamAdapter) Unsubscribe(ctx *dgctx.DgContext, topic string) error {
	_, err := a.redisCli.XGroupDestroy(topic, a.group)
	if err != nil {
		dglogger.Errorf(ctx, "XGroupDestroy error |topic:%s | err:%v", topic, err)
	}
	return err
}

func (a *redisStreamAdapter) Acknowledge(ctx *dgctx.DgContext, topic string, messageId string) error {
	_, err := a.redisCli.XAck(topic, a.group, messageId)
	if err != nil {
		dglogger.Errorf(ctx, "Acknowledge error |topic:%s | err:%v", topic, err)
	}
	return err
}
