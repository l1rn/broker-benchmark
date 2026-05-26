package main

import (
	"fmt"
	"log"

	"github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to rabbitmq")
	defer conn.Close()
	fmt.Printf("Connected to rabbitmq!\n")
	
	ch, err := conn.Channel()
	failOnError(err, "Failed to connect to channel")
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

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	var forever chan struct{}

	go func() {
		for d := range msgs {
			fmt.Printf("Received message: %s", d.Body)
		}
	}()

	<-forever
}