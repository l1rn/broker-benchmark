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
	conf   *common.BenchmarkConfig
}

func NewKafkaConsumer(conf *common.BenchmarkConfig) (*KafkaConsumer, error) {
	return &KafkaConsumer{conf: conf}, nil
}


func (c *KafkaConsumer) Run(mode string) (*common.Metrics, error) {
	total := c.conf.MessageCount
	concurrency := c.conf.Consumers
	msgSize := c.conf.MessageSize

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if total == 0 {
		cancel()
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	var received, errors int
	var latencies []time.Duration

	start := time.Now()
	groupID := fmt.Sprintf("benchmark-group-%d", time.Now().UnixNano())

	var startOffset int64

	if mode == "e2e" {
		startOffset = kafka.FirstOffset
	} else {
		startOffset = kafka.LastOffset
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:        []string{c.conf.Brokers},
				Topic:          c.conf.KafkaTopic,
				GroupID: 		groupID,
				Partition:      c.conf.KafkaPartition,
				MinBytes:       1,
				MaxBytes:       10 * 1024 * 1024,
				MaxWait: 100 * time.Millisecond,
				StartOffset:    startOffset,
			})
			defer reader.Close()

			for {
				select {
					case <-ctx.Done():
						return
					default:
				}
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
					continue
				}

				mu.Lock()
				if received >= total {
					mu.Unlock()
					return
				}
				received++
				count := received
				mu.Unlock()

				if count >= total {
					cancel()
					break
				}
			}
		}(i)
	}
	wg.Wait()
	duration := time.Since(start)

	successful := received - errors
	if successful < 0 {
		successful = 0
	}
	totalBytes := int64(successful) * int64(msgSize)
	metrics := common.ComputeMetrics(latencies, successful, duration, totalBytes)
	metrics.Errors = int(errors)

	return &metrics, nil
}
