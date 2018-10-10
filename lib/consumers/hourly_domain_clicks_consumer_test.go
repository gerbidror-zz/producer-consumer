package consumers

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/gerbidror/producer-consumerlib/distributed-cache"
	"github.com/gerbidror/producer-consumer/models"
	"fmt"
	"time"
	"github.com/bluele/go-timecop"
)

var _ = Describe("Test HourlyDomainClicksConsumer", func() {

	var (
		consumer                 *HourlyDomainClicksConsumer
		redisKey                 string
		err                      error
		outputChannel            chan *models.HourlyDomainClicks
		redisHourlyPriorityQueue *distributedcache.RedisHourlyPriorityQueue
	)

	BeforeEach(func() {
		redisKey = "test"
		outputChannel = make(chan *models.HourlyDomainClicks, 10)
		redisHourlyPriorityQueue = distributedcache.NewRedisPriorityQueue(redisKey)
		consumer = NewHourlyDomainClicksConsumer(redisHourlyPriorityQueue, outputChannel)
	})

	AfterEach(func() {
		Expect(distributedcache.RedisClientInstance.FlushAll().Err()).NotTo(HaveOccurred())
		close(outputChannel)
	})

	Describe("Test consumeHourlyDomainClicks", func() {
		var keepConsuming bool

		JustBeforeEach(func() {
			keepConsuming, err = consumer.consumeHourlyDomainClicks()
		})

		Context("when trying to produce empty value", func() {
			It("should success and have no data on output channel", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(keepConsuming).To(BeFalse())
				select {
				case _, ok := <-outputChannel:
					err := fmt.Errorf("channel should be empty, ok=%v", ok)
					Expect(err).NotTo(HaveOccurred())
				default:
				}
			})
		})

		Context("when trying to produce an existing value", func() {
			var hdc *models.HourlyDomainClicks
			BeforeEach(func() {
				fakeNowTime := time.Date(2018, time.October, 1, 1, 0, 1, 1, time.UTC)
				timecop.Freeze(fakeNowTime)
				Expect(redisHourlyPriorityQueue.Push(redisKey, fakeNowTime)).NotTo(HaveOccurred())
				hdc = &models.HourlyDomainClicks{Domain: "domain", Hour: time.Time{}, NumClicks: 5}
				Expect(distributedcache.SetUnsafeHourlyDomainValue(redisKey, hdc)).NotTo(HaveOccurred())
				fakeNowTime = fakeNowTime.Add(2 * time.Hour)
				timecop.Freeze(fakeNowTime)
			})

			It("should success and return data on output channel", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(keepConsuming).To(BeTrue())
				select {
				case val, ok := <-outputChannel:
					Expect(ok).To(BeTrue())
					Expect(val).To(Equal(hdc))
				default:
					err := fmt.Errorf("channel should not be empty")
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})
	})
})
