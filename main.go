package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"broker-benchmark/common"
	"broker-benchmark/rabbitmq"
)

func main() {
	broker := flag.String("broker", "rabbitmq", "Broker type: rabbitmq or kafka")
	mode := flag.String("mode", "producer", "Mode: producer, consumer, e2e")
	msgCount := flag.Int("count", 10000, "Number of messages")
	msgSize := flag.Int("size", 1024, "Message size in bytes")
	producers := flag.Int("producers", 1, "Number of concurrent producers")
	consumers := flag.Int("consumers", 1, "Number of concurrent consumers")
	queueTopic := flag.String("queue", "benchmark_queue", "Queue/Topic name")
	rabbitURL := flag.String("rabbit-url", "amqp://guest:guest@localhost:5672/", "RabbitMQ URL")

	flag.Parse()

	conf := &common.BenchmarkConfig{
		Broker: *broker,
		Mode: *mode,
		MessageCount: *msgCount,
		MessageSize: *msgSize,
		Producers: *producers,
		Consumers: *consumers,
		QueueTopic: *queueTopic,
		RabbitURL: *rabbitURL,
	}

	var metrics *common.Metrics
	var err error
	switch conf.Broker {
	case "rabbitmq":
		switch conf.Mode {
		case "producer":
			p, err := rabbitmq.NewRabbitProducer(conf)
			if err != nil {
				log.Fatal(err)
			}
			defer p.Close()
			metrics, err = p.Run()
		case "consumer":
			c, err := rabbitmq.NewRabbitConsumer(conf)
			if err != nil {
				log.Fatal(err)
			}
			defer c.Close()
			metrics, err = c.Run()
		case "e2e":
			metrics, err = rabbitmq.RunE2E(conf)
		default:
			log.Fatal("Unknown mode for rabbitmq")
		}
	}

	if err != nil {
		log.Fatal(err)
	}

	metrics.Print(fmt.Sprintf("%s - %s", strings.ToUpper(conf.Broker), strings.ToUpper(conf.Mode)))
}