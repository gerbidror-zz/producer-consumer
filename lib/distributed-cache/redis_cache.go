package distributedcache

import (
	"gopkg.in/redis.v5"
	"fmt"
	"github.com/gerbidror/producer-consumerlib/settings"
)

var RedisClientInstance *redis.Client
const RedisNilErrMsg = "redis: nil"

func NewRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("127.0.0.1:%d", settings.Conf.GetInt("redis_port")),
	})
}
