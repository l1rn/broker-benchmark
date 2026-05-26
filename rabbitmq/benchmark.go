package rabbitmq

import (
	"time"

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

	var prodMetrics, consMetrics *common.Metrics
	var prodErr, consErr error

	consumerReady := make(chan struct{})
	consumerDone := make(chan struct{})
	start := time.Now()
	go func() {
		consumerReady <- struct{}{}
		consMetrics, consErr = consumer.Run()
		close(consumerDone)
	}()

	<-consumerReady

	prodMetrics, prodErr = producer.Run()
	
	if prodErr != nil {
		return nil, prodErr
	}
	<-consumerDone
	if consErr != nil {
		return nil, consErr
	}

	e2eDuration := time.Since(start)
	totalBytes := int64(prodMetrics.TotalMessages) * int64(conf.MessageSize)
	metrics := common.ComputeMetrics(consMetrics.Latencies, consMetrics.TotalMessages, e2eDuration, totalBytes)
	metrics.Errors = prodMetrics.Errors + consMetrics.Errors
	return &metrics, nil
}
