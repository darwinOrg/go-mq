package dgmq_test

import (
	dgctx "github.com/darwinOrg/go-common/context"
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
		PoolSize:  20,
		Group:     "test",
		BatchSize: 10,
	})
	if err != nil {
		panic(err)
	}
	defer mqAdapter.Close()

	pubAndSub(mqAdapter, "smss_topic")
}

func pubAndSub(mqAdapter dgmq.MqAdapter, topic string) {
	ctx := &dgctx.DgContext{TraceId: "123"}
	closeCh := make(chan struct{})
	err := mqAdapter.DynamicSubscribe(ctx, closeCh, topic, func(_ *dgctx.DgContext, message string) error {
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
	close(closeCh)
	time.Sleep(time.Second)
	_ = mqAdapter.Destroy(dc, topic)
}
