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
		Addr:         kafka.TCP(conf.Brokers),
		Topic:        conf.KafkaTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequiredAcks(conf.KafkaRequiredAcks),
		BatchSize:    conf.KafkaBatchSize,
		BatchTimeout: 1 * time.Millisecond,
		Async:        false,
		Compression:  kafka.Snappy,
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
	var mu sync.Mutex
	var latencies []time.Duration
	var errors int64
	start := time.Now()

	msgsPerWorker := total / concurrency
	rem := total % concurrency

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			count := msgsPerWorker
			if workerID < rem {
				count++
			}
			messages := make([]kafka.Message, 0, p.conf.KafkaBatchSize)
			for j := 0; j < total/concurrency; j++ {
				seq := uint64(workerID*1000000 + len(latencies))
				payload := make([]byte, msgSize)
				copy(payload, basePayload)

				messages = append(messages, kafka.Message{
					Key:   []byte(fmt.Sprintf("%d", seq)),
					Value: payload,
					Time:  time.Now(),
				})

				if len(messages) >= p.conf.KafkaBatchSize {
					p.sendBatch(messages, &latencies, &errors, &mu)
					messages = messages[:0]
				}
			}
			if len(messages) > 0 {
				p.sendBatch(messages, &latencies, &errors, &mu)
			}
		}(i)
	}
	wg.Wait()
	duration := time.Since(start)
	totalBytes := int64(total) * int64(msgSize)
	metrics := common.ComputeMetrics(latencies, total-int(errors), duration, totalBytes)
	metrics.Errors = int(errors)
	return &metrics, nil
}

func (p *KafkaProducer) sendBatch(messages []kafka.Message, latencies *[]time.Duration, errors *int64, mu *sync.Mutex) {
	start := time.Now()
	err := p.writer.WriteMessages(context.Background(), messages...)
	batchLatency := time.Since(start)

	mu.Lock()
	defer mu.Unlock()

	if err != nil {
		*errors += int64(len(messages))
		fmt.Println("WRITE ERROR:", err)
		return
	}

	for i := 0; i < len(messages); i++ {
		*latencies = append(*latencies, batchLatency)
	}
}
