package distributedcache

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"fmt"
)

var _ = Describe("Test redis mutex", func() {

	var (
		err error
		key string
	)

	BeforeEach(func() {
		key = "test"
	})

	AfterEach(func() {
		Expect(RedisClientInstance.FlushAll().Err()).NotTo(HaveOccurred())
	})

	Describe("Test lock", func() {
		var isLocked bool

		JustBeforeEach(func() {
			isLocked, err = RedisMutexInstance.Lock(key)
		})

		Context("when trying to lock an unlocked key", func() {
			It("should success", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(isLocked).To(BeTrue())
			})
		})

		Context("when trying to lock a locked key", func() {
			BeforeEach(func() {
				isLocked, err = RedisMutexInstance.Lock(key)
				Expect(isLocked).To(BeTrue())
				Expect(err).NotTo(HaveOccurred())
			})

			It("should fail", func() {
				Expect(isLocked).To(BeFalse())
				Expect(err).NotTo(HaveOccurred())
			})
		})
	})

	Describe("Test unlock", func() {
		var isLocked bool

		JustBeforeEach(func() {
			err = RedisMutexInstance.Unlock(key)
		})

		Context("when trying to unlock an unlocked key", func() {
			It("should success", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when trying to lock a locked key", func() {
			BeforeEach(func() {
				isLocked, err = RedisMutexInstance.Lock(key)
				Expect(isLocked).To(BeTrue())
				Expect(err).NotTo(HaveOccurred())
			})

			It("should success", func() {
				Expect(err).NotTo(HaveOccurred())
				_, err := RedisClientInstance.Get(key).Result()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal(RedisNilErrMsg))
			})
		})
	})

	Describe("Test safeAction", func() {
		var lockKey = "mylockkey"

		JustBeforeEach(func() {
			err = RedisMutexInstance.SafeAction(lockKey, func() error { return nil })
		})

		Context("when has no previous lock", func() {
			It("should return success", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when has previous lock", func() {
			BeforeEach(func() {
				mutexLockKey := fmt.Sprintf("%s_mutex", lockKey)
				isLocked, lockErr := RedisMutexInstance.Lock(mutexLockKey)
				Expect(lockErr).NotTo(HaveOccurred())
				Expect(isLocked).To(BeTrue())
			})

			It("should return error cant obtain lock", func() {
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(ErrCantObtainLock))
			})
		})
	})
})
