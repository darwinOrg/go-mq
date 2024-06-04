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
	topic := "test"

	_ = mqAdapter.Subscribe(topic, func(_ *dgctx.DgContext, message string) error {
		log.Print(message)

		return nil
	})

	dc := &dgctx.DgContext{TraceId: uuid.NewString()}
	_ = mqAdapter.Publish(dc, topic, "hello")
	_ = mqAdapter.Publish(dc, topic, []byte("world"))
	_ = mqAdapter.Publish(dc, topic, map[string]string{"haha": "hehe"})

	time.Sleep(time.Second)
	_ = mqAdapter.Destroy(dc, topic)
	time.Sleep(time.Second * 5)
}

func TestRedisStreamAdapter(t *testing.T) {
	redisdk.InitClient("localhost:6379")
	mqAdapter := dgmq.NewRedisStreamAdapter(redisdk.GetDefaultRedisCli(), "test", os.Getenv("HOSTNAME"), 0, 10)
	topic := "test"

	_ = mqAdapter.Subscribe(topic, func(_ *dgctx.DgContext, message string) error {
		log.Print(message)

		return nil
	})

	dc := &dgctx.DgContext{TraceId: uuid.NewString()}
	_ = mqAdapter.Publish(dc, topic, "hello")
	_ = mqAdapter.Publish(dc, topic, []byte("world"))
	_ = mqAdapter.Publish(dc, topic, map[string]string{"haha": "hehe"})

	time.Sleep(time.Second)
	_ = mqAdapter.Destroy(dc, topic)
	time.Sleep(time.Second * 5)
}
