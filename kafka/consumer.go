package kafka

import (
	"context"
	"encoding/json"
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


func (c *KafkaConsumer) Run(mode string, ready chan struct{}) (*common.Metrics, error) {
	total := c.conf.MessageCount
	concurrency := c.conf.Consumers
	msgSize := c.conf.MessageSize

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex

	var received int
	var errors int
	var latencies []time.Duration

	start := time.Now()

	groupID := fmt.Sprintf("benchmark-%d", time.Now().UnixNano())

	readyOnce := sync.Once{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			fmt.Println("RECEIVED MSG")
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:   []string{c.conf.Brokers},
				Topic:     c.conf.KafkaTopic,
				GroupID:   groupID,
				MinBytes:  1,
				MaxBytes:  10e6,
				QueueCapacity: 1000,
				ReadLagInterval: -1,
			})
			defer reader.Close()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				m, err := reader.FetchMessage(ctx)
				if err != nil {
					mu.Lock()
					errors++
					mu.Unlock()
					continue
				}
				readyOnce.Do(func() {
					if ready != nil {
						close(ready)
					}
				})

				msg := decode(m.Value)
				if msg.Timestamp == 0 {
					continue
				}

				latency := time.Since(time.Unix(0, msg.Timestamp))

				mu.Lock()
				latencies = append(latencies, latency)
				received++

				if received >= total {
					cancel()
				}
				mu.Unlock()

				_ = reader.CommitMessages(ctx, m)
			}
		}()
	}

	wg.Wait()

	duration := time.Since(start)
	totalBytes := int64(received) * int64(msgSize)

	metrics := common.ComputeMetrics(latencies, received, duration, totalBytes)
	metrics.Errors = errors

	return &metrics, nil
}

func decode(b []byte) common.Message {
	var m common.Message
	_ = json.Unmarshal(b, &m)
	return m
}