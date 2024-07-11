package dgmq

import (
	"github.com/darwinOrg/go-common/constants"
	dgctx "github.com/darwinOrg/go-common/context"
	"github.com/darwinOrg/go-common/utils"
	dglogger "github.com/darwinOrg/go-logger"
	client "github.com/darwinOrg/smss-client"
	"github.com/google/uuid"
	"sync/atomic"
	"time"
)

type smssAdapter struct {
	host      string
	port      int
	group     string
	batchSize uint8
	timeout   time.Duration
	pubClient *client.PubClient
}

func NewSmssAdapter(host string, port int, group string, batchSize uint8, timeout time.Duration) (MqAdapter, error) {
	pubClient, err := client.NewPubClient(host, port, timeout)
	if err != nil {
		return nil, err
	}

	return &smssAdapter{
		host:      host,
		port:      port,
		group:     group,
		batchSize: batchSize,
		timeout:   timeout,
		pubClient: pubClient,
	}, nil
}

func (a *smssAdapter) Publish(ctx *dgctx.DgContext, topic string, message any) error {
	var payload []byte
	switch message.(type) {
	case string:
		payload = []byte(message.(string))
	case []byte:
		payload = message.([]byte)
	default:
		jsonMsg, err := utils.ConvertBeanToJsonString(message)
		if err != nil {
			dglogger.Errorf(ctx, "ConvertBeanToJsonString error | topic: %s | err: %v", topic, err)
			return err
		}

		payload = []byte(jsonMsg)
	}

	msg := client.NewMessage(payload)
	msg.AddHeader(constants.TraceId, ctx.TraceId)

	err := a.pubClient.Publish(topic, msg, ctx.TraceId)
	if err != nil {
		dglogger.Errorf(ctx, "Publish error | topic: %s | err: %v", topic, err)
	}
	return err
}

func (a *smssAdapter) Destroy(ctx *dgctx.DgContext, topic string) error {
	err := a.pubClient.DeleteMQ(topic, ctx.TraceId)
	if err != nil {
		dglogger.Errorf(ctx, "Destroy error | topic: %s | err: %v", topic, err)
	}
	return err
}

func (a *smssAdapter) Subscribe(topic string, handler SubscribeHandler) error {
	subClient, err := client.NewSubClient(topic, a.group, a.host, a.port, a.timeout)
	if err != nil {
		ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
		dglogger.Errorf(ctx, "NewSubClient error | topic: %s | err: %v", topic, err)
		return err
	}

	go func() {
		defer subClient.Close()
		a.subscribe(nil, subClient, topic, handler)
	}()

	return nil
}

func (a *smssAdapter) DynamicSubscribe(closeCh chan struct{}, topic string, handler SubscribeHandler) error {
	ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
	err := a.pubClient.CreateMQ(topic, 0, ctx.TraceId)
	if err != nil {
		dglogger.Errorf(ctx, "pubClient.CreateMQ error | topic: %s | err: %v", topic, err)
		return err
	}
	subClient, err := client.NewSubClient(topic, a.group, a.host, a.port, a.timeout)
	if err != nil {
		ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
		dglogger.Errorf(ctx, "NewSubClient error | topic: %s | err: %v", topic, err)
	}

	go func() {
		defer subClient.Close()
		a.subscribe(closeCh, subClient, topic, handler)
	}()

	return nil
}

func (a *smssAdapter) subscribe(closeCh chan struct{}, subClient *client.SubClient, topic string, handler SubscribeHandler) {
	end := new(atomic.Bool)
	end.Store(false)

	if closeCh != nil {
		go func() {
			<-closeCh
			end.Store(true)
		}()
	}

	err := subClient.Sub(0, a.batchSize, a.timeout, func(messages []*client.SubMessage) client.AckEnum {
		for _, msg := range messages {
			traceId := msg.GetHeaderValue(constants.TraceId)
			ctx := &dgctx.DgContext{TraceId: utils.IfReturn(traceId != "", traceId, uuid.NewString())}
			message := string(msg.ToBytes())
			handlerErr := handler(ctx, message)
			if handlerErr != nil {
				dglogger.Errorf(ctx, "Handle fail | topic: %s | ts: %d | eventId: %d | message: %s | err: %v", topic, msg.Ts, msg.EventId, message, handlerErr)
			} else {
				dglogger.Debugf(ctx, "Handle success | topic: %s | ts: %d | eventId: %d | message: %s", topic, msg.Ts, msg.EventId, message)
			}
		}
		return utils.IfReturn(end.Load(), client.ActWithEnd, client.Ack)
	})
	if err != nil {
		ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
		dglogger.Errorf(ctx, "subClient.Sub error | topic: %s | err: %v", topic, err)
	}
}
