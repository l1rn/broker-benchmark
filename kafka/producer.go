package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"broker-benchmark/common"
)

type KafkaProducer struct {
	writer *kafka.Writer
	conf   *common.BenchmarkConfig
}

func NewKafkaProducer(conf *common.BenchmarkConfig) (*KafkaProducer, error) {
	writer := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        conf.KafkaTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequiredAcks(conf.KafkaRequiredAcks),
		BatchSize:    conf.KafkaBatchSize,
		BatchTimeout: 100 * time.Millisecond,
		Async:        true,
	}

	return &KafkaProducer{
		writer: writer,
		conf:   conf,
	}, nil
}

func (p *KafkaProducer) Close() {
	p.writer.Close()
}

func (p *KafkaProducer) Run() (*common.Metrics, error) {
	total := p.conf.MessageCount
	concurrency := p.conf.Producers
	msgSize := p.conf.MessageSize
	basePayload := make([]byte, msgSize)
	for i := range basePayload {
		basePayload[i] = 'B'
	}

	var wg sync.WaitGroup
	work := make(chan int, total)
	for i := 0; i < total; i++ {
		work <- i
	}
	close(work)

	var mu sync.Mutex
	var latencies []time.Duration
	errors := 0
	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for range work {
				mu.Lock()
				seq := uint64(workerID*1000000 + len(latencies))
				mu.Unlock()
				ts := time.Now()
				payload := make([]byte, msgSize)
				copy(payload, basePayload)

				err := p.writer.WriteMessages(
					context.Background(),
					kafka.Message{
						Key:   []byte(fmt.Sprintf("%d", seq)),
						Value: payload,
						Time:  ts,
					},
				)

				if err != nil {
					fmt.Println("WRITE ERROR:", err)
					mu.Lock()
					errors++
					mu.Unlock()
					continue
				}

				latency := time.Since(ts)
				mu.Lock()
				latencies = append(latencies, latency)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	duration := time.Since(start)
	totalBytes := int64(total) * int64(msgSize)
	metrics := common.ComputeMetrics(latencies, total-errors, duration, totalBytes)
	metrics.Errors = errors
	return &metrics, nil
}
