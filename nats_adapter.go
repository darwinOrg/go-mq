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
	subject := a.buildNatsSubject(topic)

	return dgnats.InitStream(ctx, subject)
}

func (a *natsAdapter) Publish(ctx *dgctx.DgContext, topic string, message any) error {
	return a.PublishWithTag(ctx, topic, "", message)
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

	strMsg := string(data)
	if tag != "" {
		dglogger.Infof(ctx, "Publish | topic: %s | tag: %s | message: %s", topic, tag, strMsg)
	} else {
		dglogger.Infof(ctx, "Publish | topic: %s | message: %s", topic, strMsg)
	}
	subject := a.buildNatsSubject(topic)
	err := dgnats.PublishRawWithTag(ctx, subject, tag, data)
	if err != nil {
		dglogger.Errorf(ctx, "dgnats.PublishRaw error | topic: %s | err: %v", topic, err)
	}

	return err
}

func (a *natsAdapter) Destroy(ctx *dgctx.DgContext, topic string) error {
	subject := a.buildNatsSubject(topic)
	return dgnats.DeleteStream(ctx, subject)
}

func (a *natsAdapter) Subscribe(ctx *dgctx.DgContext, topic string, handler SubscribeHandler) (SubscribeEndCallback, error) {
	return a.SubscribeWithTag(ctx, topic, "", handler)
}

func (a *natsAdapter) SubscribeWithTag(ctx *dgctx.DgContext, topic, tag string, handler SubscribeHandler) (SubscribeEndCallback, error) {
	subject := a.buildNatsSubject(topic)

	sub, err := dgnats.SubscribeRawWithTag(ctx, subject, tag, func(ctx *dgctx.DgContext, bytes []byte) error {
		message := string(bytes)
		if tag != "" {
			dglogger.Infof(ctx, "Subscribe | topic: %s | tag: %s | message: %s", topic, tag, message)
		} else {
			dglogger.Infof(ctx, "Subscribe | topic: %s | message: %s", topic, message)
		}
		return handler(ctx, message)
	})
	if err != nil {
		return nil, err
	}

	return func() { _ = sub.Unsubscribe() }, nil
}

func (a *natsAdapter) DynamicSubscribe(ctx *dgctx.DgContext, closeCh chan struct{}, topic string, handler SubscribeHandler) error {
	if closeCh != nil {
		go func() {
			<-closeCh
			dglogger.Infof(ctx, "closed topic: %s ", topic)
		}()
	}

	_, err := a.Subscribe(ctx, topic, handler)
	return err
}

func (a *natsAdapter) Unsubscribe(ctx *dgctx.DgContext, topic string) error {
	subject := a.buildNatsSubject(topic)

	return dgnats.Unsubscribe(ctx, subject, "")
}

func (a *natsAdapter) UnsubscribeWithTag(ctx *dgctx.DgContext, topic, tag string) error {
	subject := a.buildNatsSubject(topic)

	return dgnats.Unsubscribe(ctx, subject, tag)
}

func (a *natsAdapter) Close() {
	dgnats.Close()
}

func (a *natsAdapter) buildNatsSubject(topic string) *dgnats.NatsSubject {
	name := dgnats.ReplaceIllegalCharacter(topic)

	return &dgnats.NatsSubject{
		Category: name,
		Name:     name,
		Group:    a.group,
	}
}
