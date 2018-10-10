package distributedcache

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
	"github.com/bluele/go-timecop"
	"gopkg.in/redis.v5"
	"fmt"
)

var _ = Describe("Test redis hourly priority queue mutex", func() {

	var (
		err           error
		priorityQueue *RedisHourlyPriorityQueue
		redisKey      string
		fakeNowTime   time.Time
	)

	BeforeEach(func() {
		redisKey = "lockKey"
		fakeNowTime = time.Date(2018, time.October, 1, 1, 0, 1, 1, time.UTC)
		timecop.Freeze(fakeNowTime)
		priorityQueue = NewRedisPriorityQueue(redisKey)
		priorityQueue.additionalMinutes = 1
	})

	AfterEach(func() {
		Expect(RedisClientInstance.FlushAll().Err()).NotTo(HaveOccurred())
		timecop.Return()
	})

	Describe("Test push", func() {
		var (
			scoreTime time.Time
			hourlyKey string
		)

		BeforeEach(func() {
			hourlyKey = "hourlyKey"
		})

		JustBeforeEach(func() {
			err = priorityQueue.Push(hourlyKey, scoreTime)
		})

		Context("when pushing non existing value", func() {
			BeforeEach(func() {
				scoreTime = timecop.Now().Add(1*time.Hour + 10*time.Minute)
			})

			It("should success and return empty data", func() {
				Expect(err).NotTo(HaveOccurred())
				score := priorityQueue.convertTimeToPriorityScore(scoreTime)
				zRangeBy := redis.ZRangeBy{Min: "0", Max: fmt.Sprintf("%v", score+1), Offset: 0, Count: 10}
				values, err := RedisClientInstance.ZRangeByScoreWithScores(priorityQueueKey, zRangeBy).Result()
				Expect(err).NotTo(HaveOccurred())
				expectedVal := redis.Z{
					Score:  score,
					Member: hourlyKey,
				}
				Expect(values).To(Equal([]redis.Z{expectedVal}))
			})
		})

		Context("when pushing existing value with newer time", func() {
			var (
				oldScore float64
				maxScore float64
			)

			BeforeEach(func() {
				scoreTime = timecop.Now().Add(1*time.Hour + 10*time.Minute)
				oldScore = priorityQueue.convertTimeToPriorityScore(scoreTime)
				oldVal := redis.Z{
					Score:  oldScore,
					Member: hourlyKey,
				}
				Expect(RedisClientInstance.ZAdd(priorityQueueKey, oldVal).Err()).NotTo(HaveOccurred())
				scoreTime = timecop.Now().Add(1*time.Hour + 20*time.Minute)
				maxScore = priorityQueue.convertTimeToPriorityScore(scoreTime)
			})

			It("should success and keep score", func() {
				Expect(err).NotTo(HaveOccurred())
				zRangeBy := redis.ZRangeBy{Min: "0", Max: fmt.Sprintf("%v", maxScore+1), Offset: 0, Count: 10}
				values, err := RedisClientInstance.ZRangeByScoreWithScores(priorityQueueKey, zRangeBy).Result()
				Expect(err).NotTo(HaveOccurred())
				expectedVal := redis.Z{Score: oldScore, Member: hourlyKey}
				Expect(values).To(Equal([]redis.Z{expectedVal}))
			})
		})

		Context("when pushing existing value with older time", func() {
			var (
				oldScore float64
				maxScore float64
			)

			BeforeEach(func() {
				scoreTime = timecop.Now().Add(1*time.Hour + 20*time.Minute)
				maxScore = priorityQueue.convertTimeToPriorityScore(scoreTime)
				oldScore = priorityQueue.convertTimeToPriorityScore(scoreTime)
				oldVal := redis.Z{
					Score:  oldScore,
					Member: hourlyKey,
				}
				Expect(RedisClientInstance.ZAdd(priorityQueueKey, oldVal).Err()).NotTo(HaveOccurred())
				scoreTime = timecop.Now().Add(1*time.Hour + 10*time.Minute)
			})

			It("should success and keep score", func() {
				Expect(err).NotTo(HaveOccurred())
				zRangeBy := redis.ZRangeBy{Min: "0", Max: fmt.Sprintf("%v", maxScore+1), Offset: 0, Count: 10}
				values, err := RedisClientInstance.ZRangeByScoreWithScores(priorityQueueKey, zRangeBy).Result()
				Expect(err).NotTo(HaveOccurred())
				newScore := priorityQueue.convertTimeToPriorityScore(timecop.Now().Add(1*time.Hour + 10*time.Minute))
				expectedVal := redis.Z{Score: newScore, Member: hourlyKey}
				Expect(values).To(Equal([]redis.Z{expectedVal}))
			})
		})

		Context("when pushing new value with non relevant value existing", func() {
			BeforeEach(func() {
				oldVal := redis.Z{
					Score:  100.0,
					Member: "bbbb",
				}
				Expect(RedisClientInstance.ZAdd(priorityQueueKey, oldVal).Err()).NotTo(HaveOccurred())
				scoreTime = timecop.Now().Add(1*time.Hour + 20*time.Minute)
			})

			It("should success and keep score", func() {
				Expect(err).NotTo(HaveOccurred())
				score := priorityQueue.convertTimeToPriorityScore(scoreTime)
				zRangeBy := redis.ZRangeBy{Min: "0", Max: fmt.Sprintf("%v", score+1), Offset: 0, Count: 10}
				values, err := RedisClientInstance.ZRangeByScoreWithScores(priorityQueueKey, zRangeBy).Result()
				Expect(err).NotTo(HaveOccurred())
				expectedVal := []redis.Z{{100.0, "bbbb"}, {score, hourlyKey}}
				Expect(values).To(Equal(expectedVal))
			})
		})
	})

	Describe("Test pop", func() {
		var keyResult string

		JustBeforeEach(func() {
			keyResult, err = priorityQueue.Pop()
		})

		Context("when popping data from empty list", func() {
			It("should return empty value", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(keyResult).To(Equal(""))
				zRangeBy := redis.ZRangeBy{Min: "0", Max: "60", Offset: 0, Count: 10}
				values, err := RedisClientInstance.ZRangeByScoreWithScores(priorityQueueKey, zRangeBy).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(values).To(Equal([]redis.Z{}))
			})
		})

		Context("when has priority data with score of time plus 1 hour and 10 minutes, and 1 hour and 20 minutes", func() {
			var zValues []redis.Z

			BeforeEach(func() {
				scoreTimes := []time.Time{timecop.Now().Add(1*time.Hour + 10*time.Minute), timecop.Now().Add(1*time.Hour + 20*time.Minute)}
				members := []string{"a1", "a2"}

				zValues = make([]redis.Z, 0, 2)
				for i := 0; i < 2; i++ {
					zValue := redis.Z{Score: priorityQueue.convertTimeToPriorityScore(scoreTimes[i]), Member: members[i]}
					Expect(RedisClientInstance.ZAdd(priorityQueueKey, zValue).Err()).NotTo(HaveOccurred())
					zValues = append(zValues, zValue)
				}
			})

			Context("when popping with priority of time.Now() plus 1 hour and 30 minutes", func() {
				BeforeEach(func() {
					newTime := timecop.Now().Add(1*time.Hour + 30*time.Minute)
					timecop.Freeze(newTime)
				})

				It("should return empty value", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(keyResult).To(Equal(""))
					score := priorityQueue.convertTimeToPriorityScore(timecop.Now())
					zRangeBy := redis.ZRangeBy{Min: "0", Max: fmt.Sprintf("%v", score+1), Offset: 0, Count: 10}
					values, err := RedisClientInstance.ZRangeByScoreWithScores(priorityQueueKey, zRangeBy).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(values).To(Equal(zValues))
				})
			})

			Context("when popping with priority of time.Now() plus 2 hours", func() {
				BeforeEach(func() {
					newTime := timecop.Now().Add(2 * time.Hour)
					timecop.Freeze(newTime)
				})

				It("should return empty value (should wait for additional time period)", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(keyResult).To(Equal(""))
					score := priorityQueue.convertTimeToPriorityScore(timecop.Now())
					zRangeBy := redis.ZRangeBy{Min: "0", Max: fmt.Sprintf("%v", score+1), Offset: 0, Count: 10}
					values, err := RedisClientInstance.ZRangeByScoreWithScores(priorityQueueKey, zRangeBy).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(values).To(Equal(zValues))
				})
			})

			Context("when popping with priority of time.Now() plus 2 hours and 1 minute", func() {
				BeforeEach(func() {
					newTime := timecop.Now().Add(2*time.Hour + time.Minute)
					timecop.Freeze(newTime)
				})

				It("should return first value", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(keyResult).To(Equal("a1"))
					score := priorityQueue.convertTimeToPriorityScore(timecop.Now())
					zRangeBy := redis.ZRangeBy{Min: "0", Max: fmt.Sprintf("%v", score+1), Offset: 0, Count: 10}
					values, err := RedisClientInstance.ZRangeByScoreWithScores(priorityQueueKey, zRangeBy).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(values).To(Equal([]redis.Z{zValues[1]}))
				})
			})
		})

		Context("when has priority data with score of exactly round hour and hour pop score is that round hour (max boundary check)", func() {
			var zValue redis.Z

			BeforeEach(func() {
				scoreTime := timecop.Now().Add(2 * time.Hour)
				member := "a1"
				zValue = redis.Z{Score: priorityQueue.convertTimeToPriorityScore(scoreTime), Member: member}
				Expect(RedisClientInstance.ZAdd(priorityQueueKey, zValue).Err()).NotTo(HaveOccurred())
				newTime := timecop.Now().Add(2*time.Hour + 5*time.Minute)
				timecop.Freeze(newTime)
			})

			It("should return empty value", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(keyResult).To(Equal(""))
				score := priorityQueue.convertTimeToPriorityScore(timecop.Now())
				zRangeBy := redis.ZRangeBy{Min: "0", Max: fmt.Sprintf("%v", score+1), Offset: 0, Count: 10}
				values, err := RedisClientInstance.ZRangeByScoreWithScores(priorityQueueKey, zRangeBy).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(values).To(Equal([]redis.Z{zValue}))
			})
		})

		Context("when has the same priority data and we need to pop that data", func() {
			var zValues []redis.Z

			BeforeEach(func() {
				scoreTimes := []time.Time{timecop.Now().Add(1*time.Hour + 10*time.Minute), timecop.Now().Add(1*time.Hour + 10*time.Minute)}
				members := []string{"a1", "a2"}
				zValues = make([]redis.Z, 0, 2)
				for i := 0; i < 2; i++ {
					zValue := redis.Z{Score: priorityQueue.convertTimeToPriorityScore(scoreTimes[i]), Member: members[i]}
					Expect(RedisClientInstance.ZAdd(priorityQueueKey, zValue).Err()).NotTo(HaveOccurred())
					zValues = append(zValues, zValue)
				}
				newTime := timecop.Now().Add(2*time.Hour + 5*time.Minute)
				timecop.Freeze(newTime)
			})

			It("should return first inserted value (a1)", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(keyResult).To(Equal("a1"))
				score := priorityQueue.convertTimeToPriorityScore(timecop.Now())
				zRangeBy := redis.ZRangeBy{Min: "0", Max: fmt.Sprintf("%v", score+1), Offset: 0, Count: 10}
				values, err := RedisClientInstance.ZRangeByScoreWithScores(priorityQueueKey, zRangeBy).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(values).To(Equal([]redis.Z{zValues[1]}))
			})
		})

	})
})
