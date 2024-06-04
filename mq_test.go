package dgmq_test

import (
	"context"
	dgctx "github.com/darwinOrg/go-common/context"
	dgmq "github.com/darwinOrg/go-mq"
	redisdk "github.com/darwinOrg/go-redis"
	"github.com/google/uuid"
	"log"
	"testing"
	"time"
)

func TestRedisAdapter(t *testing.T) {
	redisdk.InitClient("localhost:6379")
	mqAdapter := &dgmq.RedisListAdapter{
		RedisCli: redisdk.GetDefaultRedisCli(),
		Timeout:  time.Minute,
	}

	topic := "test"

	ctx := context.Background()
	_ = mqAdapter.Subscribe(ctx, topic, func(_ *dgctx.DgContext, message any) error {
		msg := message.(string)
		if len(msg) > 0 {
			log.Print(msg)
		}

		return nil
	})

	dc := &dgctx.DgContext{TraceId: uuid.NewString()}
	_ = mqAdapter.Publish(dc, topic, "hello world")
	_ = mqAdapter.Publish(dc, topic, "haha")

	time.Sleep(time.Second)
	_ = mqAdapter.Destroy(dc, topic)
}
