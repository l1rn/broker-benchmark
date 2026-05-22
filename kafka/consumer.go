package kafka

import (
	"context"
	"fmt"
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
	return &KafkaConsumer{conf: conf}, nil
}


func (c *KafkaConsumer) Run() (*common.Metrics, error) {
	total := c.conf.MessageCount
	concurrency := c.conf.Consumers
	msgSize := c.conf.MessageSize

	var wg sync.WaitGroup
	var mu sync.Mutex

	var received int64
	var errors int64
	var latencies []time.Duration
	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:        []string{c.conf.Brokers},
				Topic:          c.conf.KafkaTopic,
				GroupID: 		fmt.Sprintf("benchmark-group-%d", workerID),
				Partition:      c.conf.KafkaPartition,
				MinBytes:       1,
				MaxBytes:       10 * 1024 * 1024,
				MaxWait: 500 * time.Millisecond,
				StartOffset:    kafka.FirstOffset,
				CommitInterval: 1 * time.Second,
			})
			defer reader.Close()

			for atomicReceived := int64(0); atomicReceived < int64(total); {
				msg, err := reader.FetchMessage(context.Background())
				if err != nil {
					mu.Lock()
					errors++
					mu.Unlock()
					continue
				}

				if !msg.Time.IsZero(){
					latency := time.Since(msg.Time)
					mu.Lock()
					latencies = append(latencies, latency)
					mu.Unlock()
				}

				if err := reader.CommitMessages(context.Background(), msg); err != nil {
					mu.Lock()
					errors++
					mu.Unlock()
				}

				mu.Lock()
				received++
				count := received
				mu.Unlock()

				if count%5000 == 0 {
					fmt.Printf("Consumed: %d / %d\n", count, total)
				}

				if count >= int64(total) {
					break
				}
			}
		}(i)
	}
	wg.Wait()
	duration := time.Since(start)

	totalBytes := int64(received) * int64(msgSize)
	metrics := common.ComputeMetrics(latencies, int(received)-int(errors), duration, totalBytes)
	metrics.Errors = int(errors)

	return &metrics, nil
}
