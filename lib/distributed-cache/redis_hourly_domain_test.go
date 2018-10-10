package distributedcache

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/gerbidror/producer-consumer/models"
	"time"
	"encoding/json"
)

var _ = Describe("Test redis hourly domain", func() {
	AfterEach(func() {
		Expect(RedisClientInstance.FlushAll().Err()).NotTo(HaveOccurred())
	})

	Describe("Test GetHourlyDomainKey", func() {
		var (
			domain string
			key    string
			hour   int64
		)

		JustBeforeEach(func() {
			key = GetHourlyDomainKey(domain, hour)
		})

		Context("when has valid data", func() {
			BeforeEach(func() {
				domain = "a"
				hour = 1
			})

			It("should success", func() {
				Expect(key).To(Equal("hourly_domain_a_1"))
			})
		})
	})

	Describe("Test SetUnsafeHourlyDomainValue", func() {
		var (
			key string
			hdc *models.HourlyDomainClicks
			err error
		)

		JustBeforeEach(func() {
			err = SetUnsafeHourlyDomainValue(key, hdc)
		})

		Context("when has valid data", func() {
			BeforeEach(func() {
				key = "a"
				hdc = &models.HourlyDomainClicks{
					Domain:    "domain",
					Hour:      time.Time{},
					NumClicks: 5,
				}
			})

			It("should success", func() {
				Expect(err).NotTo(HaveOccurred())
				bytes, err := RedisClientInstance.Get(key).Bytes()
				Expect(err).NotTo(HaveOccurred())
				temp := &models.HourlyDomainClicks{}
				err = json.Unmarshal(bytes, &temp)
				Expect(err).NotTo(HaveOccurred())
				Expect(temp).To(Equal(hdc))
			})
		})
	})

	Describe("Test GetUnsafeHourlyDomainValue", func() {
		var (
			key      string
			hdc      *models.HourlyDomainClicks
			expected *models.HourlyDomainClicks
			err      error
		)

		JustBeforeEach(func() {
			hdc, err = GetUnsafeHourlyDomainValue(key)
		})

		Context("when key not exists", func() {
			BeforeEach(func() {
				key = "a"
			})

			It("should success", func() {
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal(RedisNilErrMsg))
			})
		})

		Context("when has valid data", func() {
			BeforeEach(func() {
				key = "a"
				expected = &models.HourlyDomainClicks{
					Domain:    "domain",
					Hour:      time.Time{},
					NumClicks: 5,
				}
				marshaledVal, err := json.Marshal(*expected)
				Expect(err).NotTo(HaveOccurred())
				Expect(RedisClientInstance.Set(key, marshaledVal, 0).Err()).NotTo(HaveOccurred())
			})

			It("should success", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(expected).To(Equal(hdc))
			})
		})
	})
})
