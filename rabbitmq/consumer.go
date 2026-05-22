package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"broker-benchmark/common"
)

type RabbitConsumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   string
	conf    *common.BenchmarkConfig
}

func NewRabbitConsumer(conf *common.BenchmarkConfig) (*RabbitConsumer, error) {
	conn, err := amqp.Dial(conf.RabbitURL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	_, err = ch.QueueDeclare(
		conf.QueueTopic,
		true,
		false,
		false,
		false,
		nil,
	)

	if err := ch.Qos(conf.Consumers*20, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}
	
	return &RabbitConsumer{
		conn:    conn,
		channel: ch,
		queue:   conf.QueueTopic,
		conf:    conf,
	}, nil
}

func (c *RabbitConsumer) Close() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *RabbitConsumer) Run() (*common.Metrics, error) {
	total := c.conf.MessageCount
	concurrency := c.conf.Consumers
	var wg sync.WaitGroup
	var mu sync.Mutex
	var received, errors int64
	var latencies []time.Duration

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
				fmt.Sprintf("consuver-%d", workerID),
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
						atomic.AddInt64(&errors, 1)
					}

					if atomic.AddInt64(&received, 1) >= int64(total) {
						cancel()
						return 
					}
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)
	
	totalReceived := int(atomic.LoadInt64(&received))
	totalErrors := int(atomic.LoadInt64(&errors))

	totalBytes := int64(total) * int64(c.conf.MessageSize)
	metrics := common.ComputeMetrics(latencies, totalReceived-totalErrors, duration, totalBytes)
	metrics.Errors = totalErrors
	
	return &metrics, nil
}
