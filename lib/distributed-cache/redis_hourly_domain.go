package distributedcache

import (
	"fmt"
	"github.com/gerbidror/producer-consumer/models"
	"encoding/json"
)

func GetHourlyDomainKey(domain string, hour int64) string {
	return fmt.Sprintf("hourly_domain_%s_%d", domain, hour)
}

func GetUnsafeHourlyDomainValue(key string) (*models.HourlyDomainClicks, error) {
	stringCmd := RedisClientInstance.Get(key)

	bytes, err := stringCmd.Bytes()
	if err != nil {
		return &models.HourlyDomainClicks{}, err
	}

	if bytes == nil {
		return nil, nil
	}

	hdc := models.HourlyDomainClicks{}
	if err = json.Unmarshal(bytes, &hdc); err != nil {
		return &models.HourlyDomainClicks{}, err
	}

	return &hdc, nil
}

func SetUnsafeHourlyDomainValue(key string, hdc *models.HourlyDomainClicks) error {
	marshaledVal, err := json.Marshal(*hdc)
	if err != nil {
		return err
	}

	if err := RedisClientInstance.Set(key, marshaledVal, 0).Err(); err != nil {
		return err
	}

	return nil
}

func PurgeUnsafeHourlyDomainValue(key string) error {
	if err := RedisClientInstance.Del(key).Err(); err != nil {
		return err
	}

	return nil
}
