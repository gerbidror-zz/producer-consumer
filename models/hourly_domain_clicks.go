package models

import "time"

type HourlyDomainClicks struct {
	Domain    string
	Hour      time.Time
	NumClicks int
}
