package dgmq

import (
	dgctx "github.com/darwinOrg/go-common/context"
	"github.com/darwinOrg/go-common/utils"
	dglogger "github.com/darwinOrg/go-logger"
	redisdk "github.com/darwinOrg/go-redis"
	"github.com/google/uuid"
	"time"
)

type redisListAdapter struct {
	redisCli redisdk.RedisCli
	timeout  time.Duration
}

func NewRedisListAdapter(redisCli redisdk.RedisCli, config *MqAdapterConfig) MqAdapter {
	return &redisListAdapter{
		redisCli: redisCli,
		timeout:  config.Timeout,
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

	_, err := a.redisCli.LPush(topic, strMsg)
	if err != nil {
		dglogger.Errorf(ctx, "Publish error | topic: %s | err: %v", topic, err)
	}
	return err
}

func (a *redisListAdapter) Destroy(ctx *dgctx.DgContext, topic string) error {
	err := a.redisCli.Del(topic)
	if err != nil {
		dglogger.Errorf(ctx, "Destroy error | topic: %s | err: %v", topic, err)
	}
	return err
}

func (a *redisListAdapter) Subscribe(topic string, handler SubscribeHandler) error {
	go func() {
		for {
			a.subscribe(&dgctx.DgContext{TraceId: uuid.NewString()}, topic, handler)
		}
	}()

	return nil
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

func (a *redisListAdapter) SemiSubscribe(ctx *dgctx.DgContext, closeCh chan struct{}, topic string, handler SubscribeHandler) error {
	return a.DynamicSubscribe(ctx, closeCh, topic, handler)
}

func (a *redisListAdapter) subscribe(ctx *dgctx.DgContext, topic string, handler SubscribeHandler) {
	rts, readErr := a.redisCli.BRPop(a.timeout, topic)
	if readErr != nil {
		dglogger.Debugf(ctx, "BRPop error | topic: %s | err: %v", topic, readErr)
		time.Sleep(time.Second)
		return
	}
	if len(rts) == 2 {
		handlerErr := handler(ctx, rts[1])
		if handlerErr != nil {
			dglogger.Errorf(ctx, "Handle error | topic: %s | err: %v", topic, handlerErr)
		}
	}
}

func (a *redisListAdapter) Close() {

}
