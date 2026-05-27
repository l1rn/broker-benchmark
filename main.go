package main

import (
	"flag"
	"fmt"
	"log"
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
	metrics      *common.Metrics
	err          error
	startTime    int64
	flagConfig   *common.BenchmarkConfig
	textfilePath string
}

func main() {
	broker := flag.String("broker", "", "Broker type: rabbitmq or kafka")
	mode := flag.String("mode", "producer", "Mode: producer, consumer, e2e")
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
	}

	initialModel := model{
		state:        stateSelectBroker,
		choices:      []string{"rabbitmq 🐇", "kafka 🦅"},
		cursor:       0,
		flagConfig:   conf,
		textfilePath: *metricsTextfile,
	}

	if *broker != "" {
		initialModel.chosenBroker = strings.ToLower(*broker)
		initialModel.state = stateRunning
	}

	p := tea.NewProgram(initialModel)
	if _, err := p.Run(); err != nil {
		log.Fatalf("Alas, TUI ran into a problem: %v", err)
	}
}

func (m model) Init() tea.Cmd {
	if m.state == stateRunning{
		return m.runBenchmarkCmd()
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
			if m.cursor < len(m.choices)-1{
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
				return m, m.runBenchmarkCmd()
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
		for i, choice := range m.choices {
			cursor := " "
			if m.cursor == i {
				cursor = "👉"
			}
			s.WriteString(fmt.Sprintf("%s %s\n", cursor, choice))
		}
		s.WriteString("\n(Press up/down to move, enter to select, q to quit)\n")

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

func (m *model) runBenchmarkCmd() tea.Cmd {
	return func () tea.Msg  {
		m.startTime = time.Now().Unix()
		m.flagConfig.Broker = m.chosenBroker
		var metrics *common.Metrics
		var err error
		switch m.flagConfig.Broker {
			case "rabbitmq":
			switch m.flagConfig.Mode {
			case "producer":
				p, e := rabbitmq.NewRabbitProducer(m.flagConfig)
				if e != nil {
					defer p.Close()
					metrics, err = p.Run()
				} else { err = e}
			case "consumer":
				c, e := rabbitmq.NewRabbitConsumer(m.flagConfig)
				if e != nil {
					defer c.Close()
					ready := make(chan struct{})
					metrics, err = c.Run(ready)
				} else { err = e }
			case "e2e":
				metrics, err = rabbitmq.RunE2E(m.flagConfig)
			}
		case "kafka":
			if err = kafka.EnsureTopic(m.flagConfig.Brokers, m.flagConfig.KafkaTopic, m.flagConfig.Consumers); err != nil {
				switch m.flagConfig.Mode {
				case "producer":
					p, e := kafka.NewKafkaProducer(m.flagConfig)
					if e != nil {
						defer p.Close()
						metrics, err = p.Run()
					} else { err = e }
				case "consumer":
					c, e := kafka.NewKafkaConsumer(m.flagConfig)
					if err != nil {
						ready := make(chan struct{})
						metrics, err = c.Run("normal", ready)
					} else { err = e }
				case "e2e":
					metrics, err = kafka.RunE2E(m.flagConfig)
				}	
			}
		}
		
		if err == nil && m.textfilePath != "" {
			_ = common.WriteMetricsTextfile(m.textfilePath, *metrics, m.chosenBroker, m.flagConfig.Mode, m.startTime);
		}

		return benchmarkFinishedMsg{metrics: metrics, err: err}
	}
}