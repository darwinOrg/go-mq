package dgmq

import (
	"github.com/darwinOrg/go-common/constants"
	dgctx "github.com/darwinOrg/go-common/context"
	"github.com/darwinOrg/go-common/utils"
	dglogger "github.com/darwinOrg/go-logger"
	redisdk "github.com/darwinOrg/go-redis"
	"github.com/google/uuid"
	"github.com/rolandhe/smss-client/client"
	"github.com/rolandhe/smss-client/pool"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	topicExistsError = "topic exist"
	sentTimeHeader   = "sent_time"
	tagHeader        = "tag"
	smssEndHeader    = "end"
)

type smssAdapter struct {
	redisCli      redisdk.RedisCli
	host          string
	port          int
	timeout       time.Duration
	group         string
	consumer      string
	batchSize     uint8
	pubClientPool *client.PubClientPool
}

func NewSmssAdapter(redisCli redisdk.RedisCli, config *MqAdapterConfig) (MqAdapter, error) {
	if config.PoolSize < 1 {
		config.PoolSize = 1
	}
	if config.BatchSize < 1 {
		config.BatchSize = 1
	}

	pubClientPool := client.NewPubClientPool(pool.NewDefaultConfig(), config.Host, config.Port, config.Timeout)

	return &smssAdapter{
		redisCli:      redisCli,
		host:          config.Host,
		port:          config.Port,
		timeout:       config.Timeout,
		group:         config.Group,
		consumer:      config.Consumer,
		batchSize:     uint8(config.BatchSize),
		pubClientPool: pubClientPool,
	}, nil
}

func (a *smssAdapter) CreateTopic(ctx *dgctx.DgContext, topic string) error {
	return a.createTopic(ctx, topic, 0)
}

func (a *smssAdapter) createTopic(ctx *dgctx.DgContext, topic string, lifeDuration time.Duration) error {
	life := utils.IfReturn(int64(lifeDuration) == 0, 0, time.Now().Add(lifeDuration).UnixMilli())
	pubClient, err := a.pubClientPool.Borrow()
	if err != nil {
		return err
	}
	defer func() {
		_ = a.pubClientPool.Return(pubClient)
	}()
	err = pubClient.CreateTopic(topic, life, ctx.TraceId)
	if err != nil && err.Error() != topicExistsError {
		dglogger.Errorf(ctx, "CreateTopic error | topic: %s | err: %v", topic, err)
		return err
	}

	return nil
}

func (a *smssAdapter) Publish(ctx *dgctx.DgContext, topic string, message any) error {
	return a.PublishWithTag(ctx, topic, "", message)
}

func (a *smssAdapter) PublishWithTag(ctx *dgctx.DgContext, topic, tag string, message any) error {
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
	msg.AddHeader(sentTimeHeader, strconv.FormatInt(time.Now().UnixMilli(), 10))
	if tag != "" {
		msg.AddHeader(tagHeader, tag)
	}

	pubClient, err := a.pubClientPool.Borrow()
	if err != nil {
		return err
	}
	defer func() {
		_ = a.pubClientPool.Return(pubClient)
	}()

	err = pubClient.Publish(topic, msg, ctx.TraceId)
	if err != nil {
		dglogger.Errorf(ctx, "Publish error | topic: %s | tag: %s | err: %v", topic, tag, err)
	} else {
		dglogger.Debugf(ctx, "Publish success | topic: %s | tag: %s | payload: %s", topic, tag, string(payload))
	}

	return err
}

func (a *smssAdapter) Destroy(ctx *dgctx.DgContext, topic string) error {
	pubClient, err := a.pubClientPool.Borrow()
	if err != nil {
		return err
	}
	defer func() {
		_ = a.pubClientPool.Return(pubClient)
	}()

	err = pubClient.DeleteTopic(topic, ctx.TraceId)
	if err != nil {
		dglogger.Errorf(ctx, "Destroy error | topic: %s | err: %v", topic, err)
	}
	_ = a.redisCli.Del(a.getSmssEventIdKey(topic))
	return err
}

func (a *smssAdapter) Subscribe(ctx *dgctx.DgContext, topic string, handler SubscribeHandler) (SubscribeEndCallback, error) {
	return a.SubscribeWithTag(ctx, topic, "", handler)
}

func (a *smssAdapter) SubscribeWithTag(ctx *dgctx.DgContext, topic, tag string, handler SubscribeHandler) (SubscribeEndCallback, error) {
	if ctx == nil {
		ctx = &dgctx.DgContext{TraceId: uuid.NewString()}
	}
	err := a.createTopic(ctx, topic, 0)
	if err != nil {
		return nil, err
	}

	end := new(atomic.Bool)
	end.Store(false)

	subLock := NewRedisSubLock(a.redisCli, true)
	who := utils.IfReturn(tag != "", tag+"@"+a.group, a.group)
	dLockSub := client.NewDLockSub(topic, who, a.host, a.port, a.timeout, subLock)
	eventId, subFunc := a.getEventIdAndSubFunc(ctx, nil, topic, tag, 0, handler, end)
	_ = dLockSub.Sub(eventId, a.batchSize, a.timeout, subFunc, nil)

	return subLock.Shutdown, nil
}

func (a *smssAdapter) DynamicSubscribe(ctx *dgctx.DgContext, closeCh chan struct{}, topic string, handler SubscribeHandler) error {
	lifeDuration := 8 * time.Hour
	err := a.createTopic(ctx, topic, lifeDuration)
	if err != nil {
		return err
	}

	subClient, err := a.newSubClient(ctx, topic)
	if err != nil {
		return err
	}

	go func() {
		a.dynamicSubscribe(ctx, closeCh, subClient, topic, lifeDuration, handler)
	}()

	return nil
}

func (a *smssAdapter) CleanTag(ctx *dgctx.DgContext, topic, tag string) error {
	return a.redisCli.Del(a.getSmssEventIdKey(topic + "@" + tag))
}

func (a *smssAdapter) newSubClient(ctx *dgctx.DgContext, topic string) (*client.SubClient, error) {
	var subClient *client.SubClient
	var err error

	for i := 0; i < 30; i++ {
		subClient, err = client.NewSubClient(topic, a.group, a.host, a.port, a.timeout)
		if err == nil {
			break
		}
		dglogger.Errorf(ctx, "NewSubClient error | topic: %s | err: %v", topic, err)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		return nil, err
	}

	return subClient, nil
}

func (a *smssAdapter) dynamicSubscribe(ctx *dgctx.DgContext, closeCh chan struct{}, subClient *client.SubClient, topic string, lifeDuration time.Duration, handler SubscribeHandler) {
	end := new(atomic.Bool)
	end.Store(false)

	if closeCh != nil {
		go func() {
			<-closeCh
			err := a.endSub(ctx, topic, end)
			if err != nil {
				subClient.Termite()
			}
		}()
	}

	defer func() {
		if subClient != nil {
			subClient.Close()
		}
	}()

	eventId, subFunc := a.getEventIdAndSubFunc(ctx, closeCh, topic, "", lifeDuration, handler, end)
	var err error
	for {
		if subClient == nil {
			subClient, err = a.newSubClient(ctx, topic)
			if err != nil {
				dglogger.Errorf(ctx, "subClient.Sub error | topic: %s | err: %v", topic, err)
				time.Sleep(time.Second)
				continue
			}
		}

		err = subClient.Sub(eventId, a.batchSize, a.timeout, subFunc, nil)
		if err == nil {
			break
		}

		dglogger.Errorf(ctx, "subClient.Sub error | topic: %s | err: %v", topic, err)
		time.Sleep(time.Second)

		if subClient != nil {
			subClient.Close()
			subClient = nil
		}
	}
}

func (a *smssAdapter) getEventIdAndSubFunc(ctx *dgctx.DgContext, closeCh chan struct{}, topic, tag string, lifeDuration time.Duration, handler SubscribeHandler, end *atomic.Bool) (int64, client.MessagesAccept) {
	eventIdKey := a.getSmssEventIdKey(utils.IfReturn(tag != "", topic+"@"+tag, topic))
	var eventId int64
	strEventId, err := a.redisCli.Get(eventIdKey)
	if err != nil {
		dglogger.Warnf(ctx, "redisCli get smss eventid error | topic: %s | tag: %s | err: %v", topic, tag, err)
	} else {
		eventId, _ = strconv.ParseInt(strEventId, 10, 64)
		dglogger.Infof(ctx, "get smss eventid | topic: %s | tag: %s | eventId: %d", topic, tag, eventId)
	}

	return eventId, func(messages []*client.SubMessage) client.AckEnum {
		for _, msg := range messages {
			endHeader := msg.GetHeaderValue(smssEndHeader)
			if endHeader == "true" {
				dglogger.Infof(ctx, "smss client receive end message | topic: %s | tag: %s", topic, tag)
				_, _ = a.redisCli.Set(eventIdKey, strconv.FormatInt(msg.EventId, 10), lifeDuration)
				return client.AckWithEnd
			}

			msgTag := msg.GetHeaderValue(tagHeader)
			if msgTag != "" && msgTag != tag {
				_, _ = a.redisCli.Set(eventIdKey, strconv.FormatInt(msg.EventId, 10), lifeDuration)
				continue
			}

			var dc *dgctx.DgContext
			if closeCh != nil {
				dc = ctx
			} else {
				traceId := msg.GetHeaderValue(constants.TraceId)
				dc = &dgctx.DgContext{TraceId: utils.IfReturn(traceId != "", traceId, uuid.NewString())}
			}

			var delayMilli int64
			sentTime := msg.GetHeaderValue(sentTimeHeader)
			if sentTime != "" {
				sentTimeMilli, _ := strconv.ParseInt(sentTime, 10, 64)
				delayMilli = time.Now().UnixMilli() - sentTimeMilli
			}

			payload := string(msg.GetPayload())
			handlerErr := handler(dc, payload)
			if handlerErr != nil {
				dglogger.Errorf(dc, "Handle fail | topic: %s | tag: %s | ts: %d | eventId: %d | payload: %s | delayMilli: %d | err: %v",
					topic, tag, msg.Ts, msg.EventId, payload, delayMilli, handlerErr)
			} else {
				dglogger.Infof(dc, "Handle success | topic: %s | tag: %s | ts: %d | eventId: %d | payload: %s | delayMilli: %d",
					topic, tag, msg.Ts, msg.EventId, payload, delayMilli)
				_, _ = a.redisCli.Set(eventIdKey, strconv.FormatInt(msg.EventId, 10), lifeDuration)
			}
		}

		if end.Load() {
			dglogger.Infof(ctx, "smss client end flag is true | topic: %s | tag: %s", topic, tag)
			return client.AckWithEnd
		} else {
			return client.Ack
		}
	}
}

func (a *smssAdapter) endSub(ctx *dgctx.DgContext, topic string, end *atomic.Bool) error {
	dglogger.Infof(ctx, "smss client close | topic: %s", topic)
	end.Store(true)
	endMsg := client.NewMessage([]byte("{}"))
	endMsg.AddHeader(constants.TraceId, ctx.TraceId)
	endMsg.AddHeader(sentTimeHeader, strconv.FormatInt(time.Now().UnixMilli(), 10))
	endMsg.AddHeader(smssEndHeader, "true")

	requestId := ctx.GetExtraValue(RequestIdHeader)
	if strReq, ok := requestId.(string); ok && strReq != "" {
		endMsg.AddHeader(RequestIdHeader, strReq)
	}

	pubClient, err := a.pubClientPool.Borrow()
	if err != nil {
		return err
	}
	defer func() {
		_ = a.pubClientPool.Return(pubClient)
	}()

	return pubClient.Publish(topic, endMsg, ctx.TraceId)
}

func (a *smssAdapter) Close() {
	a.pubClientPool.ShutDown()
}

func (a *smssAdapter) getSmssEventIdKey(eventKey string) string {
	return "smssid_" +
		utils.IfReturn(a.group != "", a.group+"_", "") +
		utils.IfReturn(a.consumer != "", a.consumer+"_", "") +
		eventKey
}
