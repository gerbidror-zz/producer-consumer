package producers

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/gerbidror/producer-consumer/lib/distributed-cache"
	"github.com/gerbidror/producer-consumer/models"
	"time"
	"github.com/bluele/go-timecop"
)

var _ = Describe("Test HourlyDomainClicksConsumer", func() {

	var (
		consumer                 *HourlyDomainClicksProducer
		redisKey                 string
		err                      error
		redisHourlyPriorityQueue *distributedcache.RedisHourlyPriorityQueue
		fakeNowTime              time.Time
	)

	BeforeEach(func() {
		redisKey = "test"
		redisHourlyPriorityQueue = distributedcache.NewRedisPriorityQueue(redisKey)
		consumer = NewHourlyDomainClicksProducer(redisHourlyPriorityQueue, []string{})
		fakeNowTime = time.Date(2018, time.October, 1, 1, 8, 1, 1, time.UTC)
		timecop.Freeze(fakeNowTime)
	})

	AfterEach(func() {
		Expect(distributedcache.RedisClientInstance.FlushAll().Err()).NotTo(HaveOccurred())
		timecop.Return()
	})

	Describe("Test produceHourlyDomainClicks", func() {
		var pageView models.PageView

		BeforeEach(func() {
			pageView = models.PageView{UserID: 2, Domain: "px.com", Path: "/checkout", Timestamp: fakeNowTime, NumClicks: 5,}
		})

		JustBeforeEach(func() {
			err = consumer.produceHourlyDomainClicks(pageView)
		})

		Context("when producing an non existing value", func() {
			It("should return success and store pageView with truncated hour", func() {
				Expect(err).NotTo(HaveOccurred())
				fakeNowTime = time.Date(2018, time.October, 1, 3, 8, 1, 1, time.UTC)
				timecop.Freeze(fakeNowTime)
				hdc, err := distributedcache.GetUnsafeHourlyDomainValue("hourly_domain_px.com_1538355600")
				Expect(err).NotTo(HaveOccurred())
				Expect(hdc).To(Equal(&models.HourlyDomainClicks{Domain: pageView.Domain, NumClicks: pageView.NumClicks, Hour: pageView.Timestamp.Truncate(time.Hour)}))
			})
		})

		Context("when producing an existing value", func() {
			var hdc *models.HourlyDomainClicks
			BeforeEach(func() {
				fakeNowTime := time.Date(2018, time.October, 1, 1, 0, 1, 1, time.UTC)
				timecop.Freeze(fakeNowTime)
				Expect(redisHourlyPriorityQueue.Push(redisKey, fakeNowTime)).NotTo(HaveOccurred())
				hdc = &models.HourlyDomainClicks{Domain: pageView.Domain, Hour: fakeNowTime.Truncate(time.Hour), NumClicks: 5}
				Expect(distributedcache.SetUnsafeHourlyDomainValue("hourly_domain_px.com_1538355600", hdc)).NotTo(HaveOccurred())
				fakeNowTime = fakeNowTime.Add(2 * time.Hour)
				timecop.Freeze(fakeNowTime)
			})

			It("should success and return data on output channel", func() {
				Expect(err).NotTo(HaveOccurred())
				fakeNowTime = time.Date(2018, time.October, 1, 3, 8, 1, 1, time.UTC)
				timecop.Freeze(fakeNowTime)
				hdc, err := distributedcache.GetUnsafeHourlyDomainValue("hourly_domain_px.com_1538355600")
				Expect(err).NotTo(HaveOccurred())
				Expect(hdc).To(Equal(&models.HourlyDomainClicks{Domain: pageView.Domain, NumClicks: pageView.NumClicks * 2, Hour: pageView.Timestamp.Truncate(time.Hour)}))
			})
		})
	})
})
