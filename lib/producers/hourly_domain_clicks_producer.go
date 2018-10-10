package producers

import (
	"github.com/gerbidror/producer-consumer/lib/parsers"
	"github.com/gerbidror/producer-consumer/models"
	"time"
	"github.com/gerbidror/producer-consumer/lib/distributed-cache"
	"github.com/Sirupsen/logrus"
)

type HourlyDomainClicksProducer struct {
	redisHourlyPriorityQueue *distributedcache.RedisHourlyPriorityQueue
	paths                    []string
	channels                 []chan models.PageView
}

func NewHourlyDomainClicksProducer(redisHourlyPriorityQueue *distributedcache.RedisHourlyPriorityQueue, paths []string) *HourlyDomainClicksProducer {

	return &HourlyDomainClicksProducer{
		redisHourlyPriorityQueue: redisHourlyPriorityQueue,
		paths:                    paths,
		channels:                 make([]chan models.PageView, len(paths)),
	}
}

func (p *HourlyDomainClicksProducer) Start() error {

	if err := p.createChannelsWithData(); err != nil {
		return err
	}

	for _, channel := range p.channels {
		go p.produceWorker(channel)
	}

	return nil
}

func (p *HourlyDomainClicksProducer) Close() {
	for _, channel := range p.channels {
		close(channel)
	}
}

func (p *HourlyDomainClicksProducer) createChannelsWithData() error {
	for i, path := range p.paths {
		pageViews, err := parsers.NewPageViewParser(path).GetPageViews()
		if err != nil {
			return err
		}

		p.channels[i] = make(chan models.PageView, len(pageViews))
		for _, pageView := range pageViews {
			p.channels[i] <- pageView
		}
	}
	return nil
}

func (p *HourlyDomainClicksProducer) produceWorker(channel chan models.PageView) {
	for {
		select {
		case pageView := <-channel:
			if err := p.produceHourlyDomainClicks(pageView); err != nil {
				logrus.Error("failed to produce hourly domain clicks, error: ", err)
			}
		}
	}
}

func (p *HourlyDomainClicksProducer) produceHourlyDomainClicks(pageView models.PageView) error {
	hour := pageView.Timestamp.Truncate(time.Hour)
	key := distributedcache.GetHourlyDomainKey(pageView.Domain, hour.Unix())

	err := distributedcache.RedisMutexInstance.SafeAction(key, func() error {
		cachedValue, err := distributedcache.GetUnsafeHourlyDomainValue(key)
		if err != nil {
			if err.Error() == distributedcache.RedisNilErrMsg {
				cachedValue = nil
			} else {
				return err
			}
		}

		if cachedValue == nil {
			cachedValue = &models.HourlyDomainClicks{Domain: pageView.Domain, Hour: hour, NumClicks: 0}
		}

		cachedValue.NumClicks += pageView.NumClicks

		if err = distributedcache.SetUnsafeHourlyDomainValue(key, cachedValue); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	// update priority queue
	return p.redisHourlyPriorityQueue.Push(key, pageView.Timestamp)
}
