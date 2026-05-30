package common

import (
	"sync/atomic"
	"time"
)

type BenchmarkConfig struct {
	Broker string
	Mode string
	DeliveryMode string
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
	MetricsFilePath string
}

type ActiveMetrics struct {
	MessagesProcessed atomic.Uint64
	Duration time.Duration
	ThroughputMsgPS float64
	ThroughputMBPS float64
	Latencies []time.Duration
	P50 time.Duration
	P95 time.Duration
	P99 time.Duration
	Errors atomic.Int64
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
    Timestamp int64  `json:"timestamp"`
    Payload   []byte `json:"payload"`
}