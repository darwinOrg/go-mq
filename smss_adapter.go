package dgmq

import (
	"github.com/darwinOrg/go-common/constants"
	dgctx "github.com/darwinOrg/go-common/context"
	"github.com/darwinOrg/go-common/utils"
	dglogger "github.com/darwinOrg/go-logger"
	redisdk "github.com/darwinOrg/go-redis"
	client "github.com/darwinOrg/smss-client"
	"github.com/google/uuid"
	"strconv"
	"sync/atomic"
	"time"
)

type smssAdapter struct {
	redisCli  redisdk.RedisCli
	host      string
	port      int
	timeout   time.Duration
	group     string
	batchSize uint8
	pubClient *client.PubClient
}

func NewSmssAdapter(redisCli redisdk.RedisCli, config *MqAdapterConfig) (MqAdapter, error) {
	pubClient, err := client.NewPubClient(config.Host, config.Port, config.Timeout, config.PoolSize)
	if err != nil {
		return nil, err
	}

	return &smssAdapter{
		redisCli:  redisCli,
		host:      config.Host,
		port:      config.Port,
		timeout:   config.Timeout,
		group:     config.Group,
		batchSize: uint8(config.BatchSize),
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
	_ = a.redisCli.Del(getSmssEventIdKey(topic))
	return err
}

func (a *smssAdapter) Subscribe(topic string, handler SubscribeHandler) error {
	ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
	err := a.pubClient.CreateMQ(topic, 0, ctx.TraceId)
	if err != nil && err.Error() != "mq exist" {
		dglogger.Errorf(ctx, "CreateMQ error | topic: %s | err: %v", topic, err)
		return err
	}
	subClient, err := client.NewSubClient(topic, a.group, a.host, a.port, a.timeout)
	if err != nil {
		dglogger.Errorf(ctx, "NewSubClient error | topic: %s | err: %v", topic, err)
		return err
	}

	go func() {
		defer subClient.Close()
		a.subscribe(ctx, nil, subClient, topic, handler)
	}()

	return nil
}

func (a *smssAdapter) DynamicSubscribe(closeCh chan struct{}, topic string, handler SubscribeHandler) error {
	ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
	err := a.pubClient.CreateMQ(topic, time.Now().Add(8*time.Hour).UnixMilli(), ctx.TraceId)
	if err != nil && err.Error() != "mq exist" {
		dglogger.Errorf(ctx, "CreateMQ error | topic: %s | err: %v", topic, err)
		return err
	}
	subClient, err := client.NewSubClient(topic, a.group, a.host, a.port, a.timeout)
	if err != nil {
		dglogger.Errorf(ctx, "NewSubClient error | topic: %s | err: %v", topic, err)
	}

	go func() {
		defer subClient.Close()
		a.subscribe(ctx, closeCh, subClient, topic, handler)
	}()

	return nil
}

func (a *smssAdapter) subscribe(ctx *dgctx.DgContext, closeCh chan struct{}, subClient *client.SubClient, topic string, handler SubscribeHandler) {
	end := new(atomic.Bool)
	end.Store(false)

	if closeCh != nil {
		go func() {
			<-closeCh
			end.Store(true)
		}()
	}

	eventIdKey := getSmssEventIdKey(topic)
	var eventId int64
	strEventId, err := a.redisCli.Get(eventIdKey)
	if err != nil {
		dglogger.Warnf(ctx, "redisCli get smss eventid error | topic: %s | err: %v", topic, err)
	} else {
		eventId, _ = strconv.ParseInt(strEventId, 10, 64)
	}

	err = subClient.Sub(eventId, a.batchSize, a.timeout, func(messages []*client.SubMessage) client.AckEnum {
		for _, msg := range messages {
			traceId := msg.GetHeaderValue(constants.TraceId)
			dc := &dgctx.DgContext{TraceId: utils.IfReturn(traceId != "", traceId, uuid.NewString())}
			message := string(msg.GetPayload())
			handlerErr := handler(dc, message)
			if handlerErr != nil {
				dglogger.Errorf(dc, "Handle fail | topic: %s | ts: %d | eventId: %d | message: %s | err: %v", topic, msg.Ts, msg.EventId, message, handlerErr)
			} else {
				dglogger.Debugf(dc, "Handle success | topic: %s | ts: %d | eventId: %d | message: %s", topic, msg.Ts, msg.EventId, message)
				_, _ = a.redisCli.Set(eventIdKey, strconv.FormatInt(msg.EventId, 10), 0)
			}
		}
		return utils.IfReturn(end.Load(), client.ActWithEnd, client.Ack)
	})
	if err != nil {
		dglogger.Errorf(ctx, "subClient.Sub error | topic: %s | err: %v", topic, err)
	}
}

func getSmssEventIdKey(topic string) string {
	return "smssid_" + topic
}
