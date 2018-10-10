package consumers

import (
	"github.com/gerbidror/producer-consumerlib/distributed-cache"
	"time"
	"github.com/Sirupsen/logrus"
	"github.com/gerbidror/producer-consumer/models"
	"fmt"
)

type HourlyDomainClicksConsumer struct {
	redisHourlyPriorityQueue *distributedcache.RedisHourlyPriorityQueue
	ticker                   *time.Ticker
	outputChannel            chan *models.HourlyDomainClicks
}

func NewHourlyDomainClicksConsumer(redisHourlyPriorityQueue *distributedcache.RedisHourlyPriorityQueue,
	outputChannel chan *models.HourlyDomainClicks) *HourlyDomainClicksConsumer {
	return &HourlyDomainClicksConsumer{
		redisHourlyPriorityQueue: redisHourlyPriorityQueue,
		// actually it's not 100% correct to use this channel since our channel will start every minute since the startup time (using hour instead of minute will be simply wrong)
		//   it would be better to use some kind of cron task to help us start this channel every hour pass additionalMinutesToWait,
		//   but I think it's behind the scope of this assignment
		ticker:        time.NewTicker(time.Minute),
		outputChannel: outputChannel,
	}
}

func (c *HourlyDomainClicksConsumer) Start() error {
	go c.consumeWorker()

	return nil
}

func (c *HourlyDomainClicksConsumer) Close() {
	c.ticker.Stop()
}

func (c *HourlyDomainClicksConsumer) consumeWorker() {
	for {
		select {
		case <-c.ticker.C:
			var err error
			keepConsuming := true
			for keepConsuming {
				keepConsuming, err = c.consumeHourlyDomainClicks()
				if err != nil {
					logrus.Error("failed to consume hourly domain clicks, error: ", err)
					keepConsuming = false
				}
			}
		}
	}
}

func (c *HourlyDomainClicksConsumer) consumeHourlyDomainClicks() (bool, error) {
	poppedKey, err := c.redisHourlyPriorityQueue.Pop()
	if err != nil {
		return false, err
	}

	if poppedKey == "" {
		return false, nil
	}

	var hdc *models.HourlyDomainClicks
	err = distributedcache.RedisMutexInstance.SafeAction(poppedKey, func() error {
		hdc, err = distributedcache.GetUnsafeHourlyDomainValue(poppedKey)
		if err != nil {
			return err
		}
		return distributedcache.PurgeUnsafeHourlyDomainValue(poppedKey)
	})

	if err != nil {
		if err.Error() == distributedcache.RedisNilErrMsg {
			return false, nil
		}
		return false, err
	}

	fmt.Println("pop HourlyDomainClicks=", hdc)
	c.outputChannel <- hdc
	return true, nil
}
