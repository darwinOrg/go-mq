package dgmq_test

import (
	"encoding/base64"
	dgctx "github.com/darwinOrg/go-common/context"
	"github.com/darwinOrg/go-common/utils"
	dgmq "github.com/darwinOrg/go-mq"
	redisdk "github.com/darwinOrg/go-redis"
	"github.com/google/uuid"
	"log"
	"os"
	"testing"
	"time"
)

var mqAdapter dgmq.MqAdapter

func init() {
	redisdk.InitClient("localhost:6379")
	mqAdapter = &dgmq.RedisListAdapter{
		RedisCli: redisdk.GetDefaultRedisCli(),
		Group:    "test",
		Consumer: os.Getenv("HOSTNAME"),
		Block:    0,
		Count:    10,
	}
}

func TestRedisAdapter(t *testing.T) {
	ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
	topic := "test"
	subscribeData(topic)
	publishData(ctx, topic, []byte("hello world"))
	publishData(ctx, topic, []byte("haha"))
	time.Sleep(2 * time.Second)
	destroyTopic(ctx, topic)
	time.Sleep(2 * time.Second)
}

func publishData(ctx *dgctx.DgContext, topic string, data []byte) {
	message := base64.StdEncoding.EncodeToString(data)
	_ = mqAdapter.Publish(ctx, topic, map[string]any{"message": message})
}

func subscribeData(topic string) {
	utils.Retry(func() error {
		return mqAdapter.Subscribe(topic, func(_ *dgctx.DgContext, message any) error {
			msg := message.(map[string]any)
			data, _ := base64.StdEncoding.DecodeString(msg["message"].(string))
			if len(data) > 0 {
				log.Print(string(data))
			}

			return nil
		})
	}, 3, time.Second)
}

func destroyTopic(ctx *dgctx.DgContext, topic string) {
	go func() {
		utils.Retry(func() error {
			return mqAdapter.Destroy(ctx, topic)
		}, 3, time.Second)
	}()
}
