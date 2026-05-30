package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"broker-benchmark/common"
	"broker-benchmark/kafka"
	"broker-benchmark/rabbitmq"
)

type sessionState int

const (
	stateSelectBroker sessionState = iota
	stateSelectMode
	stateRunning
	stateCompleted
)

type benchmarkFinishedMsg struct {
	metrics *common.Metrics
	err     error
}

type model struct {
	state        sessionState
	choices      []string
	cursor       int
	chosenBroker string
	chosenMode   string
	metrics      *common.Metrics
	err          error
	startTime    int64
	flagConfig   *common.BenchmarkConfig
	textfilePath string
}

func main() {
	broker := flag.String("broker", "", "Broker type: rabbitmq or kafka")
	mode := flag.String("mode", "", "Mode: producer, consumer, e2e")
	deliveryMode := flag.String("delivery-mode", "persistent", "Broker delivery mode: persistent or transient")
	msgCount := flag.Int("count", 10000, "Number of messages")
	msgSize := flag.Int("size", 1024, "Message size in bytes")
	producers := flag.Int("producers", 1, "Number of concurrent producers")
	consumers := flag.Int("consumers", 1, "Number of concurrent consumers")
	queueTopic := flag.String("queue", "benchmark_queue", "Queue/Topic name")
	rabbitURL := flag.String("rabbit-url", "amqp://guest:guest@127.0.0.1:5672/", "RabbitMQ URL")
	brokers := flag.String("brokers", "localhost:29092", "Kafka brokers (comma-separated)")
	kafkaTopic := flag.String("kafka-topic", "benchmark_topic", "Kafka topic")
	kafkaPartition := flag.Int("kafka-partition", 0, "Kafka partition")
	kafkaAcks := flag.Int("kafka-acks", 1, "Kafka required acks(0, 1, -1)")
	kafkaBatch := flag.Int("kafka-batch", 1, "Kafka batch size")

	metricsTextfile := flag.String("metrics-textfile", "", "Path to Prometheus textfile")
	flag.Parse()

	conf := &common.BenchmarkConfig{
		Broker:            *broker,
		Mode:              *mode,
		DeliveryMode:      *deliveryMode,
		MessageCount:      *msgCount,
		MessageSize:       *msgSize,
		Producers:         *producers,
		Consumers:         *consumers,
		Brokers:           *brokers,
		QueueTopic:        *queueTopic,
		RabbitURL:         *rabbitURL,
		KafkaTopic:        *kafkaTopic,
		KafkaPartition:    *kafkaPartition,
		KafkaRequiredAcks: *kafkaAcks,
		KafkaBatchSize:    *kafkaBatch,
		MetricsFilePath:   *metricsTextfile,
	}

	initialModel := model{
		state:        stateSelectBroker,
		cursor:       0,
		choices:      []string{"rabbitmq 🐇", "kafka 🦅"},
		flagConfig:   conf,
		textfilePath: *metricsTextfile,
	}

	if *broker != "" {
		initialModel.chosenBroker = strings.ToLower(*broker)
		initialModel.choices = []string{"e2e", "consumer", "producer"}
	}

	if *mode != "" {
		initialModel.chosenMode = strings.ToLower(*mode)
	}

	if initialModel.chosenBroker != "" && initialModel.chosenMode != "" {
		initialModel.state = stateRunning
	}

	p := tea.NewProgram(initialModel)
	if _, err := p.Run(); err != nil {
		log.Fatalf("Alas, TUI ran into a problem: %v", err)
	}
}

func (m model) Init() tea.Cmd {
	if m.state == stateRunning {
		return runBenchmarkCmd(m.flagConfig, m.chosenBroker, m.chosenMode, m.textfilePath, time.Now().Unix())
	}

	return nil
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < len(m.choices)-1 {
				m.cursor++
			}
		case "enter":
			if m.state == stateSelectBroker {
				m.state = stateRunning
				if m.cursor == 0 {
					m.chosenBroker = "rabbitmq"
				} else {
					m.chosenBroker = "kafka"
				}
				m.state = stateSelectMode
				m.choices = []string{"consumer", "producer", "e2e"}
				m.cursor = 0
				return m, nil
			}
			if m.state == stateSelectMode {
				m.state = stateRunning
				if m.cursor == 0 {
					m.chosenMode = "e2e"
				} else if m.cursor == 1 {
					m.chosenMode = "consumer"
				} else {
					m.chosenMode = "producer"
				}
				m.state = stateRunning
				return m, runBenchmarkCmd(m.flagConfig, m.chosenBroker, m.chosenMode, m.textfilePath, time.Now().Unix())
			}
		}
	case benchmarkFinishedMsg:
		m.state = stateCompleted
		m.metrics = msg.metrics
		m.err = msg.err
		return m, tea.Quit
	}

	return m, nil

}

func (m model) View() string {
	var s strings.Builder

	s.WriteString("\n=== DISPATCHER BROKER BENCHMARK ===\n\n")

	switch m.state {
	case stateSelectBroker:
		s.WriteString("Select which middleware engine to test:\n\n")
		m.generateOutput(&s)

	case stateSelectMode:
		s.WriteString("Select which mode to use for tests:\n\n")
		slices.Reverse(m.choices)
		m.generateOutput(&s)

	case stateRunning:
		s.WriteString(fmt.Sprintf("Running %s Benchmark in Mode [%s]...\n", strings.ToUpper(m.chosenBroker), strings.ToUpper(m.flagConfig.Mode)))
		s.WriteString("Please sit back. Profiling hardware performance buffers now...\n")

	case stateCompleted:
		if m.err != nil {
			s.WriteString(fmt.Sprintf("Error during execution: %v\n", m.err))
		} else {
			s.WriteString("System test complete! Output saved safely.\n")
		}
	}

	return s.String()
}

func runBenchmarkCmd(conf *common.BenchmarkConfig, broker, mode string, textfilePath string, startTime int64) tea.Cmd {
	return func() tea.Msg {
		conf.Broker = broker
		conf.Mode = mode
		var metrics *common.Metrics
		var err error
		switch conf.Broker {
		case "rabbitmq":
			switch conf.Mode {
			case "producer":
				p, e := rabbitmq.NewRabbitProducer(conf)
				if e == nil {
					defer p.Close()
					metrics, err = p.Run()
				} else {
					err = e
				}
			case "consumer":
				c, e := rabbitmq.NewRabbitConsumer(conf)
				if e == nil {
					defer c.Close()
					ready := make(chan struct{})
					metrics, err = c.Run(ready)
				} else {
					err = e
				}
			case "e2e":
				metrics, err = rabbitmq.RunE2E(conf)
			}
		case "kafka":
			if err = kafka.EnsureTopic(conf.Brokers, conf.KafkaTopic, conf.Consumers); err == nil {
				switch conf.Mode {
				case "producer":
					p, e := kafka.NewKafkaProducer(conf)
					if e == nil {
						defer p.Close()
						metrics, err = p.Run()
					} else {
						err = e
					}
				case "consumer":
					c, e := kafka.NewKafkaConsumer(conf)
					if e == nil {
						ready := make(chan struct{})
						metrics, err = c.Run("normal", ready)
					} else {
						err = e
					}
				case "e2e":
					metrics, err = kafka.RunE2E(conf)
				}
			}
		}

		if err == nil {
			targetPath := textfilePath
			if targetPath == "" {
				targetPath = filepath.Join("shared_metrics", fmt.Sprintf("%s-%s.prom", broker, mode))
			}

			dir := filepath.Dir(targetPath)
			if errDir := os.MkdirAll(dir, 0755); errDir == nil {
				_ = common.WriteMetricsTextfile(targetPath, *metrics, broker, mode, startTime)
			}
		}

		return benchmarkFinishedMsg{metrics: metrics, err: err}
	}
}

func (m model) generateOutput(s *strings.Builder) {
	for i, choice := range m.choices {
		cursor := " "
		if m.cursor == i {
			cursor = "👉"
		}
		s.WriteString(fmt.Sprintf("%s %s\n", cursor, choice))
	}
	s.WriteString("\n(Press up/down to move, enter to select, q to quit)\n")
}
