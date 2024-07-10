package dgmq

import (
	dgctx "github.com/darwinOrg/go-common/context"
)

type SubscribeHandler func(ctx *dgctx.DgContext, message string) error

type MqAdapter interface {
	Publisher
	Subscriber
}

type Publisher interface {
	Publish(ctx *dgctx.DgContext, topic string, message any) error
	Destroy(ctx *dgctx.DgContext, topic string) error
}

type Subscriber interface {
	Subscribe(topic string, handler SubscribeHandler) error
	DynamicSubscribe(closeCh chan struct{}, topic string, handler SubscribeHandler) error
}
