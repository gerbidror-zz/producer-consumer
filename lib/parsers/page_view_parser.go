package parsers

import (
	"os"
	"bufio"
	"encoding/json"
	"github.com/gerbidror/producer-consumer/models"
)

type PageViewParser struct {
	path string
}

func NewPageViewParser(path string) *PageViewParser {
	return &PageViewParser{
		path: path,
	}
}

func (p *PageViewParser) GetPageViews() ([]models.PageView, error) {
	file, err := os.Open(p.path)
	if err != nil {
		return []models.PageView{}, err
	}
	defer file.Close()

	pageViews := make([]models.PageView, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		marshaledText := scanner.Text()

		var pageViewParser pageViewParser
		if err = json.Unmarshal([]byte(marshaledText), &pageViewParser); err != nil {
			return []models.PageView{}, err
		}

		pageView, err := pageViewParser.ToPageView()
		if err != nil {
			return []models.PageView{}, err
		}

		pageViews = append(pageViews, pageView)
	}

	if err := scanner.Err(); err != nil {
		return []models.PageView{}, err
	}

	return pageViews, nil
}
