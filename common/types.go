package common

import "time"

type BenchmarkConfig struct {
	Broker string
	Mode string
	MessageCount int
	MessageSize int
	Producers int
	Consumers int
	QueueTopic string
	Brokers string
	RabbitURL string
	KafkaTopic string
	KafkaPartition int
	KafkaRequiredAcks int
	KafkaBatchSize int
}

type Metrics struct {
	TotalMessages int
	Duration time.Duration
	ThroughputMsgPS float64
	ThroughputMBPS float64
	Latencies []time.Duration
	P50 time.Duration
	P95 time.Duration
	P99 time.Duration
	Errors int
}

type Message struct {
	Seq uint64
	Timestamp time.Time
	Payload []byte
}