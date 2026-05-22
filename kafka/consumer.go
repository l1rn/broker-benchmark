package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"broker-benchmark/common"
)

type KafkaConsumer struct {
	reader *kafka.Reader
	conf   *common.BenchmarkConfig
}

func NewKafkaConsumer(conf *common.BenchmarkConfig) (*KafkaConsumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{conf.Brokers},
		Topic:          conf.KafkaTopic,
		GroupID: "benchmark-group",
		Partition:      conf.KafkaPartition,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
		MaxWait: 1 * time.Second,
	})

	return &KafkaConsumer{
		reader: reader,
		conf:   conf,
	}, nil
}

func (c *KafkaConsumer) Close() {
	c.reader.Close()
}

func (c *KafkaConsumer) Run() (*common.Metrics, error) {
	total := c.conf.MessageCount
	concurrency := c.conf.Consumers
	var wg sync.WaitGroup
	var mu sync.Mutex
	received := 0
	var latencies []time.Duration
	errors := 0
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
		}(i)
		for received < total {
			select {
			case <-ctx.Done():
				goto done
			default:
				msg, err := c.reader.FetchMessage(ctx)
				if err != nil {
					if err == context.DeadlineExceeded{
						break
					}
					mu.Lock()
					errors++
					mu.Unlock()
					continue
				}
				if !msg.Time.IsZero() {
					latency := time.Since(msg.Time)
					mu.Lock()
					latencies = append(latencies, latency)
					mu.Unlock()
				}
				if err := c.reader.CommitMessages(context.Background(), msg); err != nil {
					errors++
				}
				mu.Lock()
				received++
				count := received
				mu.Unlock()
				
				if count%1000 == 0 {
                    println("Produced:", count, "/", total)
                }
			}
		}
	}

done:
    duration := time.Since(start)
    totalBytes := int64(total) * int64(c.conf.MessageSize)
    metrics := common.ComputeMetrics(latencies, received-errors, duration, totalBytes)
    metrics.Errors = errors
    return &metrics, nil
}
