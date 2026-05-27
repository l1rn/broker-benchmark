package rabbitmq

import (
	"context"
	"encoding/binary"
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

func (c *RabbitConsumer) Run(ready chan struct{}) (*common.Metrics, error) {
	
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
	
	readyOnce := sync.Once{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			ch, err := c.conn.Channel()
			err = ch.Qos(1000, 0, false)
			if err != nil {
				log.Printf("Worker: %d: failed to open channel: %v", workerID, err)
				mu.Lock()
				errors++
				mu.Unlock()
				return
			}
			defer ch.Close()
			
			deliveries, err := ch.Consume(
				c.queue,
				fmt.Sprintf("consumer-%d", workerID),
				true, false, false, false, nil,
			)

			if err != nil {
				log.Printf("Worker %d consume error: %v", workerID, err)
				mu.Lock()
				errors++
				mu.Unlock()
				return
			}
			readyOnce.Do(func() {
                if ready != nil {
                    close(ready)
                }
            })
			for {
				select {
				case d, ok := <-deliveries:
					if !ok { return }
					recvTime := time.Now()
					if len(d.Body) < 8 {
                        continue
                    }
					ts := int64(binary.BigEndian.Uint64(d.Body[:8]))
					latency := recvTime.Sub(time.Unix(0, ts))

					mu.Lock()
					latencies = append(latencies, latency)

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
