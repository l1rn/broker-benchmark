package rabbitmq

import (
	"encoding/binary"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"broker-benchmark/common"
)

type RabbitProducer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   string
	conf    *common.BenchmarkConfig
	LiveSuccessful atomic.Uint64
	LiveErrors atomic.Uint64
}

func NewRabbitProducer(conf *common.BenchmarkConfig) (*RabbitProducer, error) {
	conn, err := amqp.Dial(conf.RabbitURL)

	if err != nil { return nil, err }

	ch, err := conn.Channel()

	if err != nil { conn.Close(); return nil, err }

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
	if p.channel != nil { p.channel.Close() }
	if p.conn != nil { p.conn.Close() }
}

func (p *RabbitProducer) Run() (*common.Metrics, error) {
	total := p.conf.MessageCount
	concurrency := p.conf.Producers
	msgSize := p.conf.MessageSize
	
	fmt.Printf("DEBUG: Starting producer | Messages=%d | Concurrency=%d | Size=%d\n", 
        total, concurrency, msgSize)
	
    if total == 0 {
        return &common.Metrics{}, fmt.Errorf("MessageCount is 0 - check command line flags")
    }

	basePayload := make([]byte, msgSize)
	for i := range basePayload {
		basePayload[i] = 'A'
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	allLatencies := []time.Duration{}

	work := make(chan struct {}, total)
	for i := 0; i < total; i++ {
		work <- struct{}{}
	}

	close(work)

	deliveryMode := amqp.Persistent
	if (strings.ToLower(p.conf.DeliveryMode) == "transient") {
		deliveryMode = amqp.Transient
	}
	start := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			ch, err := p.conn.Channel()
			if err != nil {
				log.Printf("Worker: %d: failed to open channel: %v", workerID, err)
				p.LiveErrors.Add(1)
				return
			}

			defer ch.Close()
			if err := ch.Confirm(false); err != nil {
				log.Printf("Worker %d: confirm error: %v", workerID, err)
				p.LiveErrors.Add(1)
				return
			}
			
			confirmCh := make(chan amqp.Confirmation, 64)
			confirms := ch.NotifyPublish(confirmCh)

			for range work {
				buf := make([]byte, 8+msgSize)
				ts := time.Now().UnixNano()
				binary.BigEndian.PutUint64(buf[:8], uint64(ts))
				copy(buf[8:], basePayload)
				
				startSend := time.Now()

				err := ch.Publish(
					"",
					p.queue,
					false,
					false,
					amqp.Publishing{
						DeliveryMode: deliveryMode,
						ContentType: "application/octet-stream",
						Body:        buf,
					},
				)

				if err != nil {
					log.Printf("Worker %d: publish error: %v", workerID, err)
					p.LiveErrors.Add(1)
					continue
				}

				confirm := <-confirms
				latency := time.Since(startSend)

				if confirm.Ack {
					p.LiveSuccessful.Add(1)
					mu.Lock()
					allLatencies = append(allLatencies, latency)
					mu.Unlock()
				} else {
					p.LiveErrors.Add(1)
					fmt.Printf("Worker %d: Message NACKed\n", workerID)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)
	finalErrors := int(p.LiveErrors.Load())
	finalSuccessful := int(p.LiveSuccessful.Load())
	
	totalBytes := int64(total) * int64(msgSize)

	metrics := common.ComputeMetrics(allLatencies, finalSuccessful, duration, totalBytes)
	metrics.Errors = finalErrors
	metrics.TotalMessages = finalSuccessful
	return &metrics, nil
}
