package models

import "time"

type PageView struct {
	UserID    int       `json:"UserId"`
	Domain    string    `json:"Domain"`
	Path      string    `json:"Path"`
	Timestamp time.Time `json:"Timestamp"`
	NumClicks int       `json:"NumClicks"`
}
