package distributedcache

func init() {
	RedisClientInstance = NewRedisClient()
	RedisMutexInstance = NewRedisMutex()
}
