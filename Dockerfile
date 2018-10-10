FROM golang:onbuild
RUN mkdir /producer-consumer
ADD . /producer-consumer/
WORKDIR /producer-consumer
RUN ln -s /producer-consumer
CMD ["go", "run", "main.go"]