package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"broker-benchmark/common"
	"broker-benchmark/kafka"
	"broker-benchmark/rabbitmq"
)

func main() {
	broker := flag.String("broker", "rabbitmq", "Broker type: rabbitmq or kafka")
	mode := flag.String("mode", "producer", "Mode: producer, consumer, e2e")
	msgCount := flag.Int("count", 10000, "Number of messages")
	msgSize := flag.Int("size", 102400, "Message size in bytes")
	producers := flag.Int("producers", 1, "Number of concurrent producers")
	consumers := flag.Int("consumers", 1, "Number of concurrent consumers")
	queueTopic := flag.String("queue", "benchmark_queue", "Queue/Topic name")
	rabbitURL := flag.String("rabbit-url", "amqp://guest:guest@localhost:5672/", "RabbitMQ URL")
	brokers := flag.String("brokers", "localhost:29092", "Kafka brokers (comma-separated)")
	kafkaTopic := flag.String("kafka-topic", "benchmark_topic", "Kafka topic")
	kafkaPartition := flag.Int("kafka-partition", 0, "Kafka partition")
	kafkaAcks := flag.Int("kafka-acks", 1, "Kafka required acks(0, 1, -1)")
	kafkaBatch := flag.Int("kafka-batch", 1, "Kafka batch size")

	metricsTextfile := flag.String("metrics-textfile", "/tmp/benchmark_metrics.prom", "Path to Prometheus textfile")
	flag.Parse()

	conf := &common.BenchmarkConfig{
		Broker:            *broker,
		Mode:              *mode,
		MessageCount:      *msgCount,
		MessageSize:       *msgSize,
		Producers:         *producers,
		Consumers:         *consumers,
		Brokers:           *brokers,
		QueueTopic:        *queueTopic,
		RabbitURL:         *rabbitURL,
		KafkaTopic:        *kafkaTopic,
		KafkaPartition:    *kafkaPartition,
		KafkaRequiredAcks: *kafkaAcks,
		KafkaBatchSize:    *kafkaBatch,
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
			err = c.PurgeQueue()

			if err != nil {
				log.Fatal(err)
			}

			metrics, err = c.Run()
		case "e2e":
			metrics, err = rabbitmq.RunE2E(conf)
		default:
			log.Fatal("Unknown mode for rabbitmq")
		}
	case "kafka":
		if err := kafka.EnsureTopic(*brokers, *kafkaTopic, *consumers); err != nil {
			log.Fatalf("failed to ensure topic: %v", err)
		}
		switch conf.Mode {
		case "producer":
			p, err := kafka.NewKafkaProducer(conf)
			if err != nil {
				log.Fatal(err)
			}
			defer p.Close()
			metrics, err = p.Run()
		case "consumer":
			c, err := kafka.NewKafkaConsumer(conf)
			if err != nil {
				log.Fatal(err)
			}
			metrics, err = c.Run("normal")
		case "e2e":
			metrics, err = kafka.RunE2E(conf)
		default:
			log.Fatal("Unknown mode for kafka")
		}
	}

	if err != nil {
		log.Fatal(err)
	}

	if err := common.WriteMetricsTextfile(*metricsTextfile, *metrics, conf.Broker); err != nil {
		log.Printf("failed to write metrics textfile: %v", err)
	}
	metrics.Print(fmt.Sprintf("%s - %s", strings.ToUpper(conf.Broker), strings.ToUpper(conf.Mode)))
}
