package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"broker-benchmark/common"
)

	type KafkaConsumer struct {
		conf   *common.BenchmarkConfig
		LiveReceived atomic.Uint64
		LiveErrors atomic.Uint64
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

		var latencies []time.Duration

		start := time.Now()


		readyOnce := sync.Once{}
		for i := 0; i < concurrency; i++ {
			wg.Add(1)

			go func(workerID int) {
				defer wg.Done()
				fmt.Println("RECEIVED MSG")
				reader := kafka.NewReader(kafka.ReaderConfig{
					Brokers:   []string{c.conf.Brokers},
					Topic:     c.conf.KafkaTopic,
					Partition: workerID,
					MinBytes:  1,
					MaxBytes:  10e6,
					MaxWait: 100 * time.Millisecond,
					QueueCapacity: 1000,
				})
				defer reader.Close()

				if err := reader.SetOffset(kafka.LastOffset); err != nil {
					fmt.Printf("Worker %d failed to set offset: %v\n", workerID, err)
				}
				readyOnce.Do(func() {
					if ready != nil {
						close(ready)
					}
				})
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}

					m, err := reader.FetchMessage(ctx)
					if err != nil {
						c.LiveErrors.Add(1)
						continue
					}
					
					if len(m.Value) < 8 {
						continue
					}

					ts := int64(binary.BigEndian.Uint64(m.Value[:8]))
					latency := time.Since(time.Unix(0, ts))

					mu.Lock()
					latencies = append(latencies, latency)
					mu.Unlock()

					currentReceived := c.LiveReceived.Add(1)
					if currentReceived >= uint64(total) {
						cancel()
					}

				}
			}(i)
		}

		wg.Wait()

		duration := time.Since(start)
		finalSuccessful := c.LiveReceived.Load()
		finalErrors := c.LiveErrors.Load()
		
		totalBytes := int64(finalSuccessful) * int64(msgSize)

		metrics := common.ComputeMetrics(latencies, int(finalSuccessful) - int(finalErrors), duration, totalBytes)
		metrics.Errors = int(finalErrors)
		metrics.TotalMessages = int(finalSuccessful)

		return &metrics, nil
	}

	func decode(b []byte) common.Message {
		var m common.Message
		_ = json.Unmarshal(b, &m)
		return m
	}