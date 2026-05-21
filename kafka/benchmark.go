package kafka

import (
	"sync"
	"time"

	"broker-benchmark/common"
)

func RunE2E(conf *common.BenchmarkConfig) (*common.Metrics, error) {
	producer, err := NewKafkaProducer(conf)
	if err != nil {
		return nil, err
	}

	defer producer.Close()

	consumer, err := NewKafkaConsumer(conf)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	var wg sync.WaitGroup
	var prodMetrics, consMetrics *common.Metrics
	var prodErr, consErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		prodMetrics, prodErr = producer.Run()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(500 * time.Millisecond)
		consMetrics, consErr = consumer.Run()
	}()
	wg.Wait()
	if prodErr != nil {
		return nil, prodErr
	}

	if consErr != nil {
		return nil, consErr
	}
	combined := *consMetrics
	combined.ThroughputMsgPS = consMetrics.ThroughputMsgPS
	combined.ThroughputMBPS = consMetrics.ThroughputMBPS
	combined.Duration = prodMetrics.Duration
	return &combined, nil
}
