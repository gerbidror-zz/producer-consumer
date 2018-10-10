# Producer consumer

## Project purpose
* This is a producer consumer project.
* The producer reads files and aggregates them for the consumer.
* The files are being handled according to priority queue.

## Project configuration
Before starting this project please update conf/settings.json values as needed:
* lock_retry - integer for the number of retries in case we can't obtain redis lock.
* lock_retry_wait_time_in_millisecond - integer containing millisecond to wait when trying to obtain redis lock.
* wait_time_before_consuming_data_in_minutes - integer containing the the number of minutes to wait before each tick of the consumer.
* priority_queue_key - string the key for redis priority queue.
* consume_data_paths - string array of paths for page view data, the start position is the project.
* local_redis_port - integer containing the local port number of redis for local run (not in docker).

## Run project locally
* Make sure redis is up and running on port 6380.
* download dep package: go get -u github.com/golang/dep/cmd/dep (or "brew install dep", "brew upgrade dep")
* download the dependencies using dep: dep ensure -v 
* compile and run: "go build" + "go run main.go"

## Run project using docker
* download docker
* if needed, the port number of redis can be modified
* run: "docker-compose up"