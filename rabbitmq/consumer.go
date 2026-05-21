package rabbitmq

import (
	"fmt"
	"log"
	"sync"
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
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
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

	err = ch.Qos(conf.Consumers*10, 0, false)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}
	return &RabbitConsumer{
		conn: conn,
		channel: ch,
		queue: conf.QueueTopic,
		conf: conf,
	}, nil
}

func (c *RabbitConsumer) Close(){
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conf != nil {
		c.conn.Close()
	}
}

func (c *RabbitConsumer) Run() (*common.Metrics, error) {
	total := c.conf.MessageCount
	concurrency := c.conf.Consumers
	var wg sync.WaitGroup
	var mu sync.Mutex
	received := 0
	var latencies []time.Duration
	errors := 0
	
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
			for d := range deliveries {
				if !d.Timestamp.IsZero() {
					latency := time.Since(d.Timestamp)
					mu.Lock()
					latencies = append(latencies, latency)
					mu.Unlock()
				}


				if err := d.Ack(false); err != nil {
					mu.Lock()
					errors++
					mu.Unlock()
				}

				mu.Lock()
				received++
				cnt := received
				mu.Unlock()

				if cnt >= total {
					break
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