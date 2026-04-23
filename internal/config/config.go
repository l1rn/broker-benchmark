package config

import "time"

type Config struct {
	RabbitMQ  RabbitMQConfig  `yaml:"rabbitmq"`
	Kafka     KafkaConfig     `yaml:"kafka"`
	Benchmark BenchmarkConfig `yaml:benchmark`
}

type RabbitMQConfig struct {
	URL       string `yaml:"url"`
	QueueName string `yaml:"queue_name"`
	Exchange  string `yaml:"exchange"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
	GroupID string   `yaml:"group_id"`
}

type BenchmarkConfig struct {
	MessageCount int           `yaml:"message_count"`
	MessageSizes []int         `yaml:"message_sizes"`
	Workers      int           `yaml:"workers"`
	Duration     time.Duration `yaml:"duration"`
	WarmupTime   time.Duration `yaml:"warmup_time"`
}

func DefaultConfig() *Config {
	return &Config{
		RabbitMQ: RabbitMQConfig{
			URL:       "amqp://guest:guest@localhost:5672",
			QueueName: "benchmark_queue",
			Exchange:  "",
		},
		Kafka: KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "benchmark_topic",
			GroupID: "benchmark_group",
		},
		Benchmark: BenchmarkConfig{
			MessageCount: 10000,
			MessageSizes: []int{100, 1024, 10240},
			Workers:      10,
			Duration:     30 * time.Second,
			WarmupTime:   5 * time.Second,
		},
	}
}
