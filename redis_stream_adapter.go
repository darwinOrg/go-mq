package dgmq

import (
	dgctx "github.com/darwinOrg/go-common/context"
	"github.com/darwinOrg/go-common/utils"
	dglogger "github.com/darwinOrg/go-logger"
	redisdk "github.com/darwinOrg/go-redis"
	"time"
)

const (
	defaultRedisStreamKey = "###redis_stream###"
)

type redisStreamAdapter struct {
	group    string
	consumer string
	block    time.Duration
	count    int64
}

func NewRedisStreamAdapter(config *MqAdapterConfig) MqAdapter {
	return &redisStreamAdapter{
		group:    config.Group,
		consumer: config.Consumer,
		block:    config.Timeout,
		count:    config.BatchSize,
	}
}

func (a *redisStreamAdapter) CreateTopic(ctx *dgctx.DgContext, topic string) error {
	return nil
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

	_, err := redisdk.XAdd(topic, values)
	if err != nil {
		dglogger.Errorf(ctx, "XAdd error | topic: %s | err: %v", topic, err)
	}
	return err
}

func (a *redisStreamAdapter) PublishWithTag(ctx *dgctx.DgContext, topic, tag string, message any) error {
	return a.Publish(ctx, topic+"@"+tag, message)
}

func (a *redisStreamAdapter) Destroy(ctx *dgctx.DgContext, topic string) error {
	_, err := redisdk.Del(topic)
	if err != nil {
		dglogger.Errorf(ctx, "Destroy error | topic: %s | err: %v", topic, err)
	}
	return err
}

func (a *redisStreamAdapter) Subscribe(ctx *dgctx.DgContext, topic string, handler SubscribeHandler) (SubscribeEndCallback, error) {
	_, err := redisdk.XGroupCreateMkStream(topic, a.group)
	if err != nil {
		return nil, err
	}

	end := false
	go func() {
		for {
			if end {
				break
			}

			a.subscribe(ctx, topic, handler)
		}
	}()

	return func() {
		end = true
	}, nil
}

func (a *redisStreamAdapter) SubscribeWithTag(ctx *dgctx.DgContext, topic, tag string, handler SubscribeHandler) (SubscribeEndCallback, error) {
	return a.Subscribe(ctx, topic+"@"+tag, handler)
}

func (a *redisStreamAdapter) DynamicSubscribe(ctx *dgctx.DgContext, closeCh chan struct{}, topic string, handler SubscribeHandler) error {
	_, err := redisdk.XGroupCreateMkStream(topic, a.group)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-closeCh:
				dglogger.Infof(ctx, "closed topic: %s ", topic)
				return
			default:
				a.subscribe(ctx, topic, handler)
			}
		}
	}()

	return nil
}

func (a *redisStreamAdapter) CleanTag(ctx *dgctx.DgContext, topic, tag string) error {
	return nil
}

func (a *redisStreamAdapter) subscribe(ctx *dgctx.DgContext, topic string, handler SubscribeHandler) {
	xstreams, readErr := redisdk.XReadGroup(topic, a.group, a.consumer, a.block, a.count)
	if readErr != nil {
		dglogger.Errorf(ctx, "XReadGroup error | topic:%s | err:%v", topic, readErr)
		time.Sleep(time.Second)
		return
	}

	for _, xstream := range xstreams {
		for _, xmessage := range xstream.Messages {
			message, ok := xmessage.Values[defaultRedisStreamKey].(string)
			if !ok {
				continue
			}
			handlerErr := handler(ctx, message)
			if handlerErr != nil {
				dglogger.Errorf(ctx, "Handle error | topic:%s | err:%v", topic, handlerErr)
				continue
			}

			_, err := redisdk.XAck(topic, a.group, xmessage.ID)
			if err != nil {
				dglogger.Errorf(ctx, "Acknowledge error |topic:%s | err:%v", topic, err)
			}
		}
	}

	return
}

func (a *redisStreamAdapter) Close() {

}
