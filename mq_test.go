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
	mqAdapter := dgmq.NewRedisListAdapter(redisdk.GetDefaultRedisCli(), time.Minute)
	pubAndSub(mqAdapter)
}

func TestRedisStreamAdapter(t *testing.T) {
	redisdk.InitClient("localhost:6379")
	mqAdapter := dgmq.NewRedisStreamAdapter(redisdk.GetDefaultRedisCli(), "test", os.Getenv("HOSTNAME"), 0, 10)
	pubAndSub(mqAdapter)
}

func TestSmssAdapter(t *testing.T) {
	redisdk.InitClient("localhost:6379")
	mqAdapter, err := dgmq.NewSmssAdapter("localhost", 12301, time.Second*5, 20, os.Getenv("HOSTNAME"), 5, redisdk.GetDefaultRedisCli())
	if err != nil {
		panic(err)
	}
	pubAndSub(mqAdapter)
}

func pubAndSub(mqAdapter dgmq.MqAdapter) {
	topic := "test"
	closeCh := make(chan struct{})
	err := mqAdapter.DynamicSubscribe(closeCh, topic, func(_ *dgctx.DgContext, message string) error {
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
	//_ = mqAdapter.Destroy(dc, topic)
}
