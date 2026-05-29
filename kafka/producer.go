package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"broker-benchmark/common"
)

type KafkaProducer struct {
	writer *kafka.Writer
	conf   *common.BenchmarkConfig
	LiveSent atomic.Uint64
	LiveErrors atomic.Uint64
}

func NewKafkaProducer(conf *common.BenchmarkConfig) (*KafkaProducer, error) {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(conf.Brokers),
		Topic:        conf.KafkaTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 100 * time.Millisecond,
		Async:        false,
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

	fmt.Println("Using topic:", p.conf.KafkaTopic)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var latencies []time.Duration
	start := time.Now()

	msgsPerWorker := total / concurrency
	rem := total % concurrency

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			count := msgsPerWorker

			if workerID < rem { count++ }
			batchSize := 1000
			batch := make([]kafka.Message, 0, batchSize)

			for j := 0; j < count; j++ {
				buf := make([]byte, 8+len(basePayload))
				ts := time.Now().UnixNano()
				binary.BigEndian.PutUint64(buf[:8], uint64(ts))
				copy(buf[8:], basePayload)
				batch = append(batch, kafka.Message{
					Key:   []byte(fmt.Sprintf("%d-%d", workerID, j)),
					Value: buf,
				})

				if len(batch) == batchSize || j == count-1 {
					startSend := time.Now()
					
					err := p.writer.WriteMessages(context.Background(), batch...)
					lat := time.Since(startSend)
					
					mu.Lock()
					if err != nil {
						p.LiveErrors.Add(uint64(len(batch)))
					} else {
						p.LiveSent.Add(uint64(len(batch)))
						for k := 0; k < len(batch); k++ {
							latencies = append(latencies, lat)
						}
					}
					mu.Unlock()
					
					batch = batch[:0]
				}
			}
		}(i)
	}
	wg.Wait()
	duration := time.Since(start)
	totalBytes := int64(total) * int64(msgSize)
	finalSuccessful := p.LiveSent.Load()
	finalErrors := p.LiveErrors.Load()
	
	metrics := common.ComputeMetrics(latencies, int(finalSuccessful-finalErrors), duration, totalBytes)
	
	metrics.Errors = int(finalErrors)
	metrics.TotalMessages = int(finalSuccessful)
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