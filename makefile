.PHONY: build clean run-rabbit-producer run-rabbit-consumer run-kafka-produce run-kafka-consumer

build:
	go mod tiny
	go build -o bin/benchmark main.go

clean:
	rm -rf bin/

run-rabbit-producer: build
	./bin/benchmark -broker rabbitmq -mode producer -count 100000 -size 1024 -producers 4

run-rabbit-consumer: build
	./bin/benchmark -broker rabbitmq -mode consumer -count 100000 -size 1024 -consumers 4

run-kafka-producer: build
	./bin/benchmark -broker kafka -mode producer -count 100000 -size 1024 -producers 4

run-kafka-consumer: build
	./bin/benchmark -broker kafka -mode consumer -count 100000 -size 1024 -consumers 4

un-rabbit-e2e: build
	./bin/benchmark -broker rabbitmq -mode e2e -count 50000 -size 512 -producers 2 -consumers 2

run-kafka-e2e: build
	./bin/benchmark -broker kafka -mode e2e -count 50000 -size 512 -producers 2 -consumers 2 -brokers localhost:9092