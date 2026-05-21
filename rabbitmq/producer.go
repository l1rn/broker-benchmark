package rabbitmq

import (
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"broker-benchmark/common"
)

type RabbitProducer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   string
	conf    *common.BenchmarkConfig
}

func NewRabbitProducer(conf *common.BenchmarkConfig) (*RabbitProducer, error) {
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

	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}

	if err := ch.Confirm(false); err != nil {
		return nil, err
	}

	return &RabbitProducer{
		conn:    conn,
		channel: ch,
		queue:   conf.QueueTopic,
		conf:    conf,
	}, nil
}

func (p *RabbitProducer) Close() {
	if p.channel != nil {
		p.channel.Close()
	}
	if p.conf != nil {
		p.conn.Close()
	}
}

func (p *RabbitProducer) Run() (*common.Metrics, error) {
	total := p.conf.MessageCount
	concurrency := p.conf.Producers
	msgSize := p.conf.MessageSize

	basePayload := make([]byte, msgSize)
	for i := range basePayload {
		basePayload[i] = 'A'
	}

	var wg sync.WaitGroup
	work := make(chan int, total)
	for i := 0; i < total; i++ {
		work <- i
	}
	close(work)

	var mu sync.Mutex
	allLatencies := []time.Duration{}
	errors := 0

	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			ch, err := p.conn.Channel()
			if err != nil {
				log.Printf("Worker: %d: failed to open channel: %v", workerID, err)
				mu.Lock()
				errors++
				mu.Unlock()
				return
			}

			defer ch.Close()
			if err := ch.Confirm(false); err != nil {
				log.Printf("Worker %d: confirm error: %v", workerID, err)
				mu.Lock()
				errors++
				mu.Unlock()
				return
			}

			confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

			for range work {
				ts := time.Now()
				payload := make([]byte, msgSize)
				copy(payload, basePayload)

				err := ch.Publish(
					"",
					p.queue,
					false,
					false,
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        payload,
						Timestamp:   ts,
					},
				)

				if err != nil {
					log.Printf("Worker %d: publish error: %v", workerID, err)
					mu.Lock()
					errors++
					mu.Unlock()
					continue
				}

				confirm := <-confirms
				if confirm.Ack {
					latency := time.Since(ts)
					mu.Lock()
					allLatencies = append(allLatencies, latency)
					mu.Unlock()
				} else {
					mu.Lock()
					errors++
					mu.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)
	totalBytes := int64(total) * int64(msgSize)
	metrics := common.ComputeMetrics(allLatencies, total-len(allLatencies)-errors, duration, totalBytes)
	metrics.Errors = errors
	return &metrics, nil
}
