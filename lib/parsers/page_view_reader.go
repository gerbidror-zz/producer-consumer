package parsers

import (
	"time"
	"github.com/gerbidror/producer-consumer/models"
)

const pageViewTimestamp = "2006-01-02 15:04:05"

type pageViewParser struct {
	UserID    int    `json:"UserId"`
	Domain    string `json:"Domain"`
	Path      string `json:"Path"`
	Timestamp string `json:"Timestamp"`
	NumClicks int    `json:"NumClicks"`
}

func (p *pageViewParser) ToPageView() (models.PageView, error) {
	t, err := time.Parse(pageViewTimestamp, p.Timestamp)
	if err != nil {
		return models.PageView{}, err
	}
	pageView := models.PageView{
		UserID:    p.UserID,
		Domain:    p.Domain,
		Path:      p.Path,
		Timestamp: t,
		NumClicks: p.NumClicks,
	}
	return pageView, nil
}
