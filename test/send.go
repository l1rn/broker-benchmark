package main

import (
	"context"
	"fmt"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s", msg, err)
	}
}

func main(){
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to rabbitmq") 
	defer conn.Close()
	fmt.Printf("Connected to rabbitmq!\n")

	ch, err := conn.Channel()
	failOnError(err, "Failed to create channel")
	defer ch.Close()
	fmt.Printf("Connected to channel!\n")

	q, err := ch.QueueDeclare(
		"hello",
		true,
		false, 
		false,
		false,
		amqp091.Table{
			amqp091.QueueTypeArg: amqp091.QueueTypeQuorum,
		},
	)
	failOnError(err, "Failed to declare a queue")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := "Hello World!"
	err = ch.PublishWithContext(ctx,
		"",
		q.Name,
		false,
		false,
		amqp091.Publishing{
			ContentType: "plain/text",
			Body: []byte(body),
		},
	)
	failOnError(err, "Failed to publish a message")
}