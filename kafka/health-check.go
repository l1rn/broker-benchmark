package kafka

import (
	"fmt"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

func EnsureTopic(brokers, topic string, numPartitions int) error {
	conn, err := kafka.Dial("tcp", brokers)
	if err != nil {
		return fmt.Errorf("kafka dial: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp",
		net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic: topic,
		NumPartitions: numPartitions,
		ReplicationFactor: 1,
	})
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	return nil
}

func DeleteTopic(brokers, topic string) error {
	conn, err := kafka.Dial("tcp", brokers)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	controllerConn, err := kafka.Dial("tcp",
		net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	return controllerConn.DeleteTopics(topic)
}