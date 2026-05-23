package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"broker-benchmark/common"
)

type RabbitConsumer struct {
	conn    *amqp.Connection
	queue   string
	conf    *common.BenchmarkConfig
}

func NewRabbitConsumer(conf *common.BenchmarkConfig) (*RabbitConsumer, error) {
	conn, err := amqp.Dial(conf.RabbitURL)
	if err != nil {
		return nil, err
	}

	return &RabbitConsumer{
		conn:    conn,
		queue:   conf.QueueTopic,
		conf:    conf,
	}, nil
}

func (c *RabbitConsumer) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *RabbitConsumer) Run() (*common.Metrics, error) {
	
	total := c.conf.MessageCount
	concurrency := c.conf.Consumers

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if total == 0 {
		cancel()
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var received, errors int
	var latencies []time.Duration

	start := time.Now()
	

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			ch, err := c.conn.Channel()
			if err != nil {
				log.Printf("Worker: %d: failed to open channel: %v", workerID, err)
				mu.Lock()
				errors++
				mu.Unlock()
				return
			}
			defer ch.Close()

			if err := ch.Qos(50, 0, false); err != nil {
				log.Printf("Consumer %d QoS error: %v", workerID, err)
			}

			deliveries, err := ch.Consume(
				c.queue,
				fmt.Sprintf("consumer-%d", workerID),
				false, false, false, false, nil,
			)

			if err != nil {
				log.Printf("Worker %d consume error: %v", workerID, err)
				mu.Lock()
				errors++
				mu.Unlock()
				return
			}

			for {
				select {
				case d, ok := <-deliveries:
					if !ok {
						return
					}

					recvTime := time.Now()
					if !d.Timestamp.IsZero() {
						latency := recvTime.Sub(d.Timestamp)
						mu.Lock()
						latencies = append(latencies, latency)
						mu.Unlock()
					}

					if err := d.Ack(false); err != nil {
						mu.Lock()
						errors++
						mu.Unlock()
						continue
					}

					mu.Lock()
					received++
					if received >= total {
						cancel()
					}
					mu.Unlock()
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	totalBytes := int64(total) * int64(c.conf.MessageSize)
	metrics := common.ComputeMetrics(latencies, received-errors, duration, totalBytes)
	metrics.Errors = errors

	return &metrics, nil
}

func (c *RabbitConsumer) PurgeQueue() error {
	ch, err := c.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	_, err = ch.QueuePurge(c.queue, false)
	return err
}
