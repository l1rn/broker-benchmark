.PHONY: build clean up down logs \
        run-rabbit-producer run-rabbit-consumer run-rabbit-e2e \
        run-kafka-producer run-kafka-consumer run-kafka-e2e

up:
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f

build:
	go mod tidy
	go build -o bin/benchmark main.go

clean:
	rm -rf bin

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
