package main

import (
	"time"
	"github.com/gerbidror/producer-consumer/lib/settings"
	"github.com/gerbidror/producer-consumer/models"
	"github.com/gerbidror/producer-consumer/lib/distributed-cache"
	"github.com/gerbidror/producer-consumer/lib/consumers"
	"github.com/gerbidror/producer-consumer/lib/producers"
	"fmt"
)

func main() {
	//fmt.Println("@@@@@@")

	priorityQueueRedisKey := settings.Conf.GetString("priority_queue_key")
	outputChannel := make(chan *models.HourlyDomainClicks, 10)
	redisHourlyPriorityQueue := distributedcache.NewRedisPriorityQueue(priorityQueueRedisKey)
	consumer := consumers.NewHourlyDomainClicksConsumer(redisHourlyPriorityQueue, outputChannel)
	paths := settings.Conf.GetStringSlice("consume_data_paths")
	producer := producers.NewHourlyDomainClicksProducer(redisHourlyPriorityQueue, paths)
	if err := consumer.Start(); err != nil {
		panic(err)
	}

	if err := producer.Start(); err != nil {
		panic(err)
	}

	time.Sleep(time.Minute * 2)
	fmt.Println("Kill consumers + producers")
	consumer.Close()
	producer.Close()
	close(outputChannel)
}
