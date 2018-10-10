package distributedcache

import (
	"time"
	"errors"
	"fmt"
	"github.com/gerbidror/producer-consumerlib/settings"
)

const mutexTTL = 2 * time.Second

type RedisMutex struct {
	lockRetry                      int
	lockRetryWaitTimeInMillisecond int
}

var RedisMutexInstance *RedisMutex

var ErrCantObtainLock = errors.New("can't obtain lock")

func NewRedisMutex() *RedisMutex {
	return &RedisMutex{
		lockRetry:                      settings.Conf.GetInt("lock_retry"),
		lockRetryWaitTimeInMillisecond: settings.Conf.GetInt("lock_retry_wait_time_in_millisecond"),
	}
}

func (m *RedisMutex) Lock(key string) (bool, error) {
	response, err := RedisClientInstance.SetNX(key, "value", mutexTTL).Result()
	if err != nil {
		return false, err
	}
	return response, nil
}

func (m *RedisMutex) Unlock(key string) error {
	_, err := RedisClientInstance.Del(key).Result()
	return err
}

func (m *RedisMutex) SafeAction(lockKey string, doAction func() error) error {

	var err error
	var isLocked bool
	lockKey = fmt.Sprintf("%s_mutex", lockKey)
	for i := 0; i < m.lockRetry; i++ {
		isLocked, err = m.Lock(lockKey)
		if err != nil {
			return err
		}

		if isLocked {
			break
		}

		time.Sleep(time.Duration(m.lockRetryWaitTimeInMillisecond) * time.Millisecond) // wait a while, and
	}

	if !isLocked {
		return ErrCantObtainLock
	}

	defer m.Unlock(lockKey)
	if err = doAction(); err != nil {
		return err
	}

	return nil
}
