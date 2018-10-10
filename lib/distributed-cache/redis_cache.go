package distributedcache

import (
	"gopkg.in/redis.v5"
	"fmt"
	"os"
	"github.com/gerbidror/producer-consumer/lib/settings"
)

var RedisClientInstance *redis.Client
const RedisNilErrMsg = "redis: nil"

func NewRedisClient() *redis.Client {
	addr := os.Getenv("REDIS_URL")
	if addr == "" {
		addr = fmt.Sprintf("localhost:%d", settings.Conf.GetInt("local_redis_port"))
	}

	return redis.NewClient(&redis.Options{
		Addr: addr,
	})
}
