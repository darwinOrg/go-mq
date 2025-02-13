package dgmq

import (
	"fmt"
	dgctx "github.com/darwinOrg/go-common/context"
	dgsys "github.com/darwinOrg/go-common/sys"
	"github.com/darwinOrg/go-common/utils"
	dglogger "github.com/darwinOrg/go-logger"
	dgnats "github.com/darwinOrg/go-nats"
	"os"
)

type natsAdapter struct {
	group string
}

func NewNatsAdapter(config *MqAdapterConfig) MqAdapter {
	ctx := dgctx.SimpleDgContext()

	natsCfg := &dgnats.NatsConfig{
		PoolSize:       config.PoolSize,
		Servers:        []string{fmt.Sprintf("nats://%s:%d", config.Host, config.Port)},
		ConnectionName: config.Username,
		Username:       config.Username,
		Password:       config.Password,
	}
	err := dgnats.Connect(natsCfg)
	if err != nil {
		dglogger.Errorf(ctx, "connect nats error: %v", err)
		if dgsys.IsFormalProfile() {
			os.Exit(1)
		}
	}

	return &natsAdapter{
		group: config.Group,
	}
}

func (a *natsAdapter) CreateTopic(ctx *dgctx.DgContext, topic string) error {
	return nil
}

func (a *natsAdapter) Publish(ctx *dgctx.DgContext, topic string, message any) error {
	var data []byte
	switch message.(type) {
	case string:
		data = []byte(message.(string))
	case []byte:
		data = message.([]byte)
	default:
		jsonMsg, err := utils.ConvertBeanToJsonString(message)
		if err != nil {
			dglogger.Errorf(ctx, "ConvertBeanToJsonString error | topic: %s | err: %v", topic, err)
			return err
		}

		data = []byte(jsonMsg)
	}

	subject := &dgnats.NatsSubject{
		Category: topic,
		Group:    a.group,
	}
	err := dgnats.PublishRaw(ctx, subject, data)
	if err != nil {
		dglogger.Errorf(ctx, "dgnats.PublishRaw error | topic: %s | err: %v", topic, err)
	}

	return err
}

func (a *natsAdapter) PublishWithTag(ctx *dgctx.DgContext, topic, tag string, message any) error {
	var data []byte
	switch message.(type) {
	case string:
		data = []byte(message.(string))
	case []byte:
		data = message.([]byte)
	default:
		jsonMsg, err := utils.ConvertBeanToJsonString(message)
		if err != nil {
			dglogger.Errorf(ctx, "ConvertBeanToJsonString error | topic: %s | err: %v", topic, err)
			return err
		}

		data = []byte(jsonMsg)
	}

	subject := &dgnats.NatsSubject{
		Category: topic,
		Name:     tag,
		Group:    a.group,
	}
	err := dgnats.PublishRaw(ctx, subject, data)
	if err != nil {
		dglogger.Errorf(ctx, "dgnats.PublishRaw error | topic: %s | err: %v", topic, err)
	}

	return err
}

func (a *natsAdapter) Destroy(ctx *dgctx.DgContext, topic string) error {
	return nil
}

func (a *natsAdapter) Subscribe(ctx *dgctx.DgContext, topic string, handler SubscribeHandler) (SubscribeEndCallback, error) {
	return func() {
	}, nil
}

func (a *natsAdapter) SubscribeWithTag(ctx *dgctx.DgContext, topic, tag string, handler SubscribeHandler) (SubscribeEndCallback, error) {
	return a.Subscribe(ctx, topic+"@"+tag, handler)
}

func (a *natsAdapter) DynamicSubscribe(ctx *dgctx.DgContext, closeCh chan struct{}, topic string, handler SubscribeHandler) error {
	return nil
}

func (a *natsAdapter) CleanTag(ctx *dgctx.DgContext, topic, tag string) error {
	return nil
}

func (a *natsAdapter) subscribe(ctx *dgctx.DgContext, topic string, handler SubscribeHandler) {

}

func (a *natsAdapter) Close() {
	dgnats.Close()
}
