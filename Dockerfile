FROM golang:onbuild
RUN mkdir /go/src/producer-consumer
ADD . /go/src/producer-consumer/
WORKDIR /go/src/producer-consumer
RUN go get -u github.com/golang/dep/cmd/dep
RUN dep ensure -v
RUN go build -o /producer-consumer/main .
CMD ["go", "run", "main.go"]