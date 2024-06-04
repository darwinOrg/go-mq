package dgmq

import (
	"context"
	dgctx "github.com/darwinOrg/go-common/context"
)

type SubscribeHandler func(ctx *dgctx.DgContext, message any) error

type MqAdapter interface {
	Publisher
	Subscriber
}

type Publisher interface {
	Publish(ctx *dgctx.DgContext, topic string, message any) error
	Destroy(ctx *dgctx.DgContext, topic string) error
}

type Subscriber interface {
	Subscribe(ctx context.Context, topic string, handler SubscribeHandler) error
	Unsubscribe(ctx *dgctx.DgContext, topic string) error
	Acknowledge(ctx *dgctx.DgContext, topic string, messageId string) error
}
