package kafka

import (
	"fmt"
	"os"
	"path/filepath"
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

	var wg sync.WaitGroup
	var prodMetrics, consMetrics *common.Metrics
	var prodErr, consErr error
	 

	consumerReady := make(chan struct{})
	targetPath := conf.MetricsFilePath
	metricName := fmt.Sprintf("%s-%s.prom", conf.Broker, conf.Mode)
	if targetPath == "" {
		targetPath = filepath.Join("shared_metrics", metricName)
	}
	_ = os.MkdirAll(filepath.Dir(targetPath), 0755)

	wg.Add(1)
	go func() {
		defer wg.Done()
		consMetrics, consErr = consumer.Run("e2e", consumerReady)
	}()

	<-consumerReady
	
	benchmarkStartTime := time.Now()
	ticker := time.NewTicker(2 * time.Second)
	done := make(chan bool)

	go func ()  {
		for {
			select {
			case <- ticker.C:
				currentReceived := consumer.LiveReceived.Load()
				elapsed := time.Since(benchmarkStartTime).Seconds()

				if elapsed <= 0 { elapsed = 1 } 

				throughputMsgPS := float64(currentReceived) / elapsed
				throughputMBPS := (throughputMsgPS * float64(conf.MessageSize)) / (1024 * 1024)

				liveMetrics := common.Metrics {
					TotalMessages:   int(currentReceived),
					ThroughputMsgPS: throughputMsgPS,
					ThroughputMBPS:  throughputMBPS,
					Duration:        time.Since(benchmarkStartTime),
				}

				_ = common.WriteMetricsTextfile(targetPath, liveMetrics, conf.Broker, conf.Mode, benchmarkStartTime.Unix())
			case <-done:
				return
			}
		}
	}()

	
	
	benchmarkStartTime = time.Now()

	wg.Add(1)
	go func() {
		defer wg.Done()
		prodMetrics, prodErr = producer.Run()
	}()

	wg.Wait()
	ticker.Stop()
	done <- true
	if prodErr != nil { return nil, prodErr }

	if consErr != nil { return nil, consErr }

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
