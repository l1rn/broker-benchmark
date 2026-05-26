package kafka

import (
	"sync"

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

	var wg sync.WaitGroup
	var prodMetrics, consMetrics *common.Metrics
	var prodErr, consErr error

	ready := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		consMetrics, consErr = consumer.Run("e2e", ready)
	}()

	<-ready

	wg.Add(1)
	go func() {
		defer wg.Done()
		prodMetrics, prodErr = producer.Run()
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
