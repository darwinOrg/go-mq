package dgmq

import (
	dgctx "github.com/darwinOrg/go-common/context"
	"github.com/darwinOrg/go-common/utils"
	dglogger "github.com/darwinOrg/go-logger"
	redisdk "github.com/darwinOrg/go-redis"
	"time"
)

type redisListAdapter struct {
	timeout time.Duration
}

func NewRedisListAdapter(config *MqAdapterConfig) MqAdapter {
	return &redisListAdapter{
		timeout: config.Timeout,
	}
}

func (a *redisListAdapter) CreateTopic(ctx *dgctx.DgContext, topic string) error {
	return nil
}

func (a *redisListAdapter) Publish(ctx *dgctx.DgContext, topic string, message any) error {
	var strMsg string
	switch message.(type) {
	case string:
		strMsg = message.(string)
	case []byte:
		strMsg = string(message.([]byte))
	default:
		jsonMsg, err := utils.ConvertBeanToJsonString(message)
		if err != nil {
			dglogger.Errorf(ctx, "ConvertBeanToJsonString error | topic: %s | err: %v", topic, err)
			return err
		}

		strMsg = jsonMsg
	}

	_, err := redisdk.LPush(topic, strMsg)
	if err != nil {
		dglogger.Errorf(ctx, "Publish error | topic: %s | err: %v", topic, err)
	}
	return err
}

func (a *redisListAdapter) PublishWithTag(ctx *dgctx.DgContext, topic, tag string, message any) error {
	return a.Publish(ctx, topic+"@"+tag, message)
}

func (a *redisListAdapter) Destroy(ctx *dgctx.DgContext, topic string) error {
	_, err := redisdk.Del(topic)
	if err != nil {
		dglogger.Errorf(ctx, "Destroy error | topic: %s | err: %v", topic, err)
	}
	return err
}

func (a *redisListAdapter) Subscribe(ctx *dgctx.DgContext, topic string, handler SubscribeHandler) (SubscribeEndCallback, error) {
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

func (a *redisListAdapter) SubscribeWithTag(ctx *dgctx.DgContext, topic, tag string, handler SubscribeHandler) (SubscribeEndCallback, error) {
	return a.Subscribe(ctx, topic+"@"+tag, handler)
}

func (a *redisListAdapter) DynamicSubscribe(ctx *dgctx.DgContext, closeCh chan struct{}, topic string, handler SubscribeHandler) error {
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

func (a *redisListAdapter) Unsubscribe(_ *dgctx.DgContext, _ string) error {
	return nil
}

func (a *redisListAdapter) UnsubscribeWithTag(_ *dgctx.DgContext, _, _ string) error {
	return nil
}

func (a *redisListAdapter) subscribe(ctx *dgctx.DgContext, topic string, handler SubscribeHandler) {
	rts, readErr := redisdk.BRPop(a.timeout, topic)
	if readErr != nil {
		dglogger.Debugf(ctx, "BRPop error | topic: %s | err: %v", topic, readErr)
		time.Sleep(time.Second)
		return
	}
	if rts != "" {
		handlerErr := handler(ctx, rts)
		if handlerErr != nil {
			dglogger.Errorf(ctx, "Handle error | topic: %s | err: %v", topic, handlerErr)
		}
	}
}

func (a *redisListAdapter) Close() {

}
