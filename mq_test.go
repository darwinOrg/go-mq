package dgmq_test

import (
	dgctx "github.com/darwinOrg/go-common/context"
	dgsys "github.com/darwinOrg/go-common/sys"
	dgmq "github.com/darwinOrg/go-mq"
	redisdk "github.com/darwinOrg/go-redis"
	"github.com/google/uuid"
	"log"
	"os"
	"testing"
	"time"
)

func TestRedisListAdapter(t *testing.T) {
	redisdk.InitClient("localhost:6379")
	mqAdapter, _ := dgmq.NewMqAdapter(&dgmq.MqAdapterConfig{
		Type:    dgmq.MqAdapterRedisList,
		Timeout: time.Minute,
	})
	defer mqAdapter.Close()

	pubAndSub(mqAdapter, "redis_list_topic")
}

func TestRedisStreamAdapter(t *testing.T) {
	redisdk.InitClient("localhost:6379")
	mqAdapter, _ := dgmq.NewMqAdapter(&dgmq.MqAdapterConfig{
		Type:      dgmq.MqAdapterRedisStream,
		Timeout:   0,
		Group:     "test",
		Consumer:  os.Getenv("HOSTNAME"),
		BatchSize: 10,
	})
	defer mqAdapter.Close()

	pubAndSub(mqAdapter, "redis_stream_topic")
}

func TestSmssAdapter(t *testing.T) {
	redisdk.InitClient("localhost:6379")
	mqAdapter, err := dgmq.NewMqAdapter(&dgmq.MqAdapterConfig{
		Type:      dgmq.MqAdapterSmss,
		Host:      "localhost",
		Port:      12301,
		Timeout:   time.Second * 5,
		PoolSize:  2,
		Group:     "test",
		BatchSize: 10,
	})
	if err != nil {
		panic(err)
	}
	defer mqAdapter.Close()

	pubAndSub(mqAdapter, "smss_topic")
}

func TestNatsAdapter(t *testing.T) {
	mqAdapter, err := dgmq.NewMqAdapter(&dgmq.MqAdapterConfig{
		Type:      dgmq.MqAdapterNats,
		Host:      "localhost",
		Port:      4222,
		Timeout:   time.Second * 5,
		PoolSize:  2,
		Group:     "test",
		BatchSize: 10,
		Username:  "startrek_mq",
		Password:  "cswjggljrmpypwfccarzpjxG-urepqldkhecvnzxzmngotaqs-bkwdvjgipruectqcowoqb6nj",
	})
	if err != nil {
		panic(err)
	}
	defer mqAdapter.Close()

	pubAndSub(mqAdapter, "nats_topic")
}

func pubAndSub(mqAdapter dgmq.MqAdapter, topic string) {
	ctx := &dgctx.DgContext{TraceId: "123"}
	tag1 := "tag1"
	tag2 := "tag2"
	cb1, err := mqAdapter.SubscribeWithTag(ctx, topic, tag1, func(_ *dgctx.DgContext, message string) error {
		log.Print(message)

		return nil
	})
	if err != nil {
		panic(err)
	}
	cb2, err := mqAdapter.SubscribeWithTag(ctx, topic, tag2, func(_ *dgctx.DgContext, message string) error {
		log.Print(message)

		return nil
	})
	if err != nil {
		panic(err)
	}
	cb3, err := mqAdapter.Subscribe(ctx, topic, func(_ *dgctx.DgContext, message string) error {
		log.Print(message)

		return nil
	})
	if err != nil {
		panic(err)
	}

	dc := &dgctx.DgContext{TraceId: uuid.NewString()}
	_ = mqAdapter.Publish(dc, topic, "hello")
	_ = mqAdapter.Publish(dc, topic, []byte("world"))
	_ = mqAdapter.Publish(dc, topic, map[string]string{"haha": "hehe"})

	time.Sleep(time.Second)
	cb1()
	cb2()
	cb3()
	dgsys.HangupApplication()
	_ = mqAdapter.CleanTag(dc, topic, tag1)
	_ = mqAdapter.CleanTag(dc, topic, tag2)
	_ = mqAdapter.Destroy(dc, topic)
}
