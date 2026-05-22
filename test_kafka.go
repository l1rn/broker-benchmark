package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/segmentio/kafka-go"
)

func main() {
    topic := "test"
    broker := "localhost:9092"

    // Подключаемся к Kafka
    conn, err := kafka.Dial("tcp", broker)
    if err != nil {
        log.Fatal("Failed to dial:", err)
    }
    defer conn.Close()

    controller, err := conn.Controller()
    if err != nil {
        log.Fatal("Failed to get controller:", err)
    }

    controllerConn, err := kafka.Dial("tcp", controller.Host+":"+fmt.Sprint(controller.Port))
    if err != nil {
        log.Fatal("Failed to dial controller:", err)
    }
    defer controllerConn.Close()

    // Удаляем топик, если существует
    err = controllerConn.DeleteTopics(topic)
    if err != nil {
        log.Println("Topic may not exist or delete failed:", err)
    }
    time.Sleep(1 * time.Second)

    // Создаём топик заново
    err = controllerConn.CreateTopics(
        kafka.TopicConfig{
            Topic:             topic,
            NumPartitions:     3,
            ReplicationFactor: 1,
        },
    )
    if err != nil {
        log.Fatal("Failed to create topic:", err)
    }
    time.Sleep(1 * time.Second)

    // Создаём продюсера
    writer := &kafka.Writer{
        Addr:         kafka.TCP(broker),
        Topic:        topic,
        Balancer:     &kafka.LeastBytes{},
        RequiredAcks: 1,
        BatchSize:    100,
        BatchTimeout: 10 * time.Millisecond,
    }
    defer writer.Close()

    // Отправляем 10 сообщений
    fmt.Println("Sending 10 messages...")
    for i := 0; i < 10; i++ {
        msg := kafka.Message{
            Key:   []byte(fmt.Sprintf("key-%d", i)),
            Value: []byte(fmt.Sprintf("message-%d", i)),
            Time:  time.Now(),
        }
        err := writer.WriteMessages(context.Background(), msg)
        if err != nil {
            log.Fatal("Failed to send:", err)
        }
        fmt.Printf("Sent message %d\n", i)
    }

    time.Sleep(1 * time.Second)

    // Читаем сообщения используя consumer group (читает из всех партиций без дублей)
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:     []string{broker},
        Topic:       topic,
        GroupID:     "test-group",
        MinBytes:    1,
        MaxBytes:    10e6,
        StartOffset: kafka.FirstOffset,
    })
    defer reader.Close()

    fmt.Println("\nReading messages with consumer group...")
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    received := 0
    for received < 10 {
        msg, err := reader.FetchMessage(ctx)
        if err != nil {
            if err == context.DeadlineExceeded {
                break
            }
            log.Println("Read error:", err)
            continue
        }
        fmt.Printf("Received: key=%s value=%s partition=%d\n",
            string(msg.Key), string(msg.Value), msg.Partition)

        reader.CommitMessages(context.Background(), msg)
        received++
    }

    fmt.Printf("\nTotal received: %d/10\n", received)
}