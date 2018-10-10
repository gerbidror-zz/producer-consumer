package distributedcache

import (
	"time"
	"gopkg.in/redis.v5"
	"github.com/bluele/go-timecop"
	"fmt"
	"github.com/gerbidror/producer-consumerlib/settings"
)

const priorityQueueKey = "priority_queue_key"
const divideByFactor = int64(1000000) // sorted set scores are saved in float64, we can't simply convert time.Now().Unix() to float64 because of buffer over flow
// we can divide it by a factor to get float64 relevant value

type RedisHourlyPriorityQueue struct {
	additionalMinutes int
	lockKey           string
}

func NewRedisPriorityQueue(redisKey string) *RedisHourlyPriorityQueue {
	waitTimeInMinutesBeforeConsumingData := settings.Conf.GetInt("wait_time_before_consuming_data_in_minutes")
	return &RedisHourlyPriorityQueue{
		additionalMinutes: waitTimeInMinutesBeforeConsumingData,
		lockKey:           redisKey,
	}
}

func (pq *RedisHourlyPriorityQueue) Push(hourlyKey string, scoreTime time.Time) error {
	z := redis.Z{
		Score:  pq.convertTimeToPriorityScore(scoreTime),
		Member: hourlyKey,
	}

	fmt.Println("push data z=", z)
	err := RedisMutexInstance.SafeAction(pq.lockKey, func() error {
		existingScore, err := RedisClientInstance.ZScore(priorityQueueKey, hourlyKey).Result()
		nilScore := false
		if err != nil {
			if err.Error() == RedisNilErrMsg {
				nilScore = true
			} else {
				return err
			}
		}

		if !nilScore && existingScore < z.Score {
			return nil
		}

		if err = RedisClientInstance.ZAdd(priorityQueueKey, z).Err(); err != nil {
			return err
		}
		return nil
	})
	return err
}

func (pq *RedisHourlyPriorityQueue) convertTimeToPriorityScore(scoreTime time.Time) float64 {
	upperBound := scoreTime.Unix() / divideByFactor
	lowerBound := scoreTime.Unix() % divideByFactor
	score := float64(upperBound) + float64(lowerBound)/float64(divideByFactor)
	return score
}

func (pq *RedisHourlyPriorityQueue) calculatePopMaxScore(scoreTime time.Time) float64 {
	scoreTime = scoreTime.Add(- time.Duration(pq.additionalMinutes)*time.Minute).Truncate(time.Hour)
	return pq.convertTimeToPriorityScore(scoreTime)
}

func (pq *RedisHourlyPriorityQueue) Pop() (string, error) {
	max := fmt.Sprintf("(%v", pq.calculatePopMaxScore(timecop.Now())) // exclude max
	zRangeBy := redis.ZRangeBy{Min: "0", Max: max, Offset: 0, Count: 10}
	var retVal string
	err := RedisMutexInstance.SafeAction(pq.lockKey, func() error {
		results, err := RedisClientInstance.ZRangeByScore(priorityQueueKey, zRangeBy).Result()
		if err != nil {
			return err
		}

		if len(results) == 0 {
			return nil
		}

		retVal = results[0]
		removeCounter, err := RedisClientInstance.ZRem(priorityQueueKey, retVal).Result()
		if err != nil {
			return err
		}

		if removeCounter == 0 {
			retVal = ""
		}

		return nil
	})

	if err != nil {
		return "", err
	}

	return retVal, nil
}
