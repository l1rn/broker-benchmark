package common

import (
	"fmt"
	"os"
	"sort"
	"time"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)


var (
	MessagesPublished = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "benchmark_messages_published_total",
			Help: "Total number of messages successfully published",
		},
		[]string{"broker"},
	)

	MessagesConsumed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "benchmark_messages_consumed_total",
			Help: "Total number of messages successfully consumed",
		},
		[]string{"broker"},
	)

	MessageLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "benchmark_message_latency_seconds",
			Help:    "End-to-end latency of messages in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"broker"},
	)

	ErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "benchmark_errors_total",
			Help: "Total number of connection or processing errors",
		},
		[]string{"broker", "type"},
	)
)
func ComputeMetrics(latencies []time.Duration, total int, duration time.Duration, totalBytes int64) Metrics{
	m := Metrics {
		TotalMessages: total,
		Duration: duration,
		Latencies: latencies,
	}

	if duration > 0 {
		m.ThroughputMsgPS = float64(total) / duration.Seconds()
		m.ThroughputMBPS = float64(totalBytes) / duration.Seconds() / (1024 * 1024)
	}

	if len(latencies) > 0 {
		sorted := make([]time.Duration, len(latencies))
		copy(sorted, latencies)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] } )
		m.P50 = sorted[int(float64(len(sorted))*0.50)]
		m.P95 = sorted[int(float64(len(sorted))*0.95)]
		m.P99 = sorted[int(float64(len(sorted))*0.99)]
	}

	return m
}

func (m Metrics) Print(title string) {
	fmt.Printf("\n=== %s ===\n", title)
	fmt.Printf("Total messages: %d\n", m.TotalMessages)
	fmt.Printf("Duration: %v\n", m.Duration)
	fmt.Printf("Throughput: %.2f msg/sec, %.2f MB/sec\n", m.ThroughputMsgPS, m.ThroughputMBPS)

	if len(m.Latencies) > 0 {
		fmt.Printf("Latency (p50): %v\n", m.P50)
		fmt.Printf("Latency (p95): %v\n", m.P95)
		fmt.Printf("Latency (p99): %v\n", m.P99)
	}

	if m.Errors > 0 {
		fmt.Printf("Errors: %d\n", m.Errors)
	}
}

func WriteMetricsTextfile(path string, m Metrics, broker string) error {
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "# HELP benchmark_total_messages Total messages processed.\n")
	fmt.Fprintf(f, "# TYPE benchmark_total_messages gauge\n")
	fmt.Fprintf(f, "benchmark_total_messages{broker=\"%s\"} %d\n", broker, m.TotalMessages)

	fmt.Fprintf(f, "# HELP benchmark_duration_seconds Benchmark duration.\n")
	fmt.Fprintf(f, "# TYPE benchmark_duration_seconds gauge\n")
	fmt.Fprintf(f, "benchmark_duration_seconds{broker=\"%s\"} %f\n", broker, m.Duration.Seconds())

	fmt.Fprintf(f, "# HELP benchmark_throughput_msg_per_sec Messages per second.\n")
	fmt.Fprintf(f, "# TYPE benchmark_throughput_msg_per_sec gauge\n")
	fmt.Fprintf(f, "benchmark_throughput_msg_per_sec{broker=\"%s\"} %.2f\n", broker, m.ThroughputMsgPS)

	fmt.Fprintf(f, "# HELP benchmark_throughput_mb_per_sec MB per second.\n")
	fmt.Fprintf(f, "# TYPE benchmark_throughput_mb_per_sec gauge\n")
	fmt.Fprintf(f, "benchmark_throughput_mb_per_sec{broker=\"%s\"} %.2f\n", broker, m.ThroughputMBPS)

	if len(m.Latencies) > 0 {
		fmt.Fprintf(f, "# HELP benchmark_latency_p50_seconds 50th percentile latency.\n")
		fmt.Fprintf(f, "# TYPE benchmark_latency_p50_seconds gauge\n")
		fmt.Fprintf(f, "benchmark_latency_p50_seconds{broker=\"%s\"} %f\n", broker, m.P50.Seconds())

		fmt.Fprintf(f, "# HELP benchmark_latency_p95_seconds 95th percentile latency.\n")
		fmt.Fprintf(f, "# TYPE benchmark_latency_p95_seconds gauge\n")
		fmt.Fprintf(f, "benchmark_latency_p95_seconds{broker=\"%s\"} %f\n", broker, m.P95.Seconds())

		fmt.Fprintf(f, "# HELP benchmark_latency_p99_seconds 99th percentile latency.\n")
		fmt.Fprintf(f, "# TYPE benchmark_latency_p99_seconds gauge\n")
		fmt.Fprintf(f, "benchmark_latency_p99_seconds{broker=\"%s\"} %f\n", broker, m.P99.Seconds())
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return nil
}