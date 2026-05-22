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

	time.Sleep(2 * time.Second)

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

	combined := &common.Metrics{
        TotalMessages:   consMetrics.TotalMessages,
        Duration:        prodMetrics.Duration,
        ThroughputMsgPS: consMetrics.ThroughputMsgPS,
        ThroughputMBPS:  consMetrics.ThroughputMBPS,
        Latencies:       consMetrics.Latencies,
        P50:             consMetrics.P50,
        P95:             consMetrics.P95,
        P99:             consMetrics.P99,
        Errors:          prodMetrics.Errors + consMetrics.Errors,
    }
	return combined, nil
}
