package parsers

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/gerbidror/producer-consumer/models"
	"time"
)

var _ = Describe("Test page view parser", func() {
	var (
		err   error
		path   string
		parser *PageViewParser
		pageViews []models.PageView
	)

	Describe("Test GetPageViews", func() {
		JustBeforeEach(func() {
			pageViews, err = parser.GetPageViews()
		})

		Context("when path exists", func() {
			BeforeEach(func() {
				path = "./test-data/test_page_view_data.txt"
				parser = NewPageViewParser(path)
			})

			It("should return success pageViews", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(len(pageViews)).To(Equal(2))
				pageViews := []models.PageView{
					{
						UserID:    2,
						Domain:    "px.com",
						Path:      "/checkout",
						Timestamp: time.Date(2018, 10, 1, 10, 0, 3,0 ,time.UTC),
						NumClicks: 5,
					},
					{
						UserID:    1,
						Domain:    "test.com",
						Path:      "/signin",
						Timestamp: time.Date(2018, 10, 1, 10, 0, 31,0 ,time.UTC),
						NumClicks: 11,
					},
				}
				Expect(pageViews).To(Equal(pageViews))
			})
		})

		Context("when path not exists", func() {
			BeforeEach(func() {
				path = "./not-exist/test_page_view_data.txt"
				parser = NewPageViewParser(path)
			})

			It("should fail", func() {
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("open ./not-exist/test_page_view_data.txt: no such file or directory"))
			})
		})
	})
})
