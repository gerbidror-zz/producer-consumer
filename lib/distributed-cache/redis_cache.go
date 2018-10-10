package distributedcache

import (
	"gopkg.in/redis.v5"
	"fmt"
	"github.com/gerbidror/producer-consumer/lib/settings"
)

var RedisClientInstance *redis.Client
const RedisNilErrMsg = "redis: nil"

func NewRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("localhost:%d", settings.Conf.GetInt("redis_port")),
	})
}
