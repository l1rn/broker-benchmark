package main

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	url := "amqp://guest:guest@localhost:5672/"
	fmt.Println("Attempting to connect to RabbitMQ")

	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	defer conn.Close()

	fmt.Println("Connected successfully!")
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

	fmt.Println("Channel opened successfully!")
	queueName := "test_queue_from_go"

	q, err := ch.QueueDeclare(
		queueName,
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	fmt.Printf("Queue '%s' created\n", q.Name)
	testMessage := fmt.Sprintf("Hello from Go at %s", time.Now().Format("15:04:55"))

	err = ch.Publish(
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(testMessage),
			Timestamp:   time.Now(),
		},
	)
	if err != nil {
		log.Fatalf("Failed to publish: %v", err)
	}

	fmt.Printf("Message published: ''%s'\n", testMessage)

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatalf("Failed to register consumer: %v", err)
	}

	fmt.Println("Waiting for message...")
	select {
	case msg := <-msgs:
		fmt.Printf("Received message: '%s'\n", string(msg.Body))
	case <-time.After(5 * time.Second):
		log.Fatal("Timeout: No message received")
	}
}
