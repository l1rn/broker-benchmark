package rabbitmq

import (
	"sync"

	"broker-benchmark/common"
)

func RunE2E(conf *common.BenchmarkConfig) (*common.Metrics, error) {
	producer, err := NewRabbitProducer(conf)
	if err != nil {
		return nil, err
	}

	defer producer.Close()

	consumer, err := NewRabbitConsumer(conf)
	if err != nil {
		return nil, err
	}

	defer consumer.Close()

	var wg sync.WaitGroup
	var prodMetrics, consMetrics *common.Metrics
	var prodErr, consErr error

	consumerReady :=  make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		go func() {
			consumerReady <- struct{}{}
		}()
		consMetrics, consErr = consumer.Run()
	}()

	<-consumerReady

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

	return &common.Metrics{
		TotalMessages:   consMetrics.TotalMessages,
		Duration:        prodMetrics.Duration,
		ThroughputMsgPS: consMetrics.ThroughputMsgPS,
		ThroughputMBPS:  consMetrics.ThroughputMBPS,
		Latencies:       consMetrics.Latencies,
		P50:             consMetrics.P50,
		P95:             consMetrics.P95,
		P99:             consMetrics.P99,
		Errors:          prodMetrics.Errors + consMetrics.Errors,
	}, nil
}
