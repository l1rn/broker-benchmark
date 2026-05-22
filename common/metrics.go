package common

import (
	"fmt"
	"sort"
	"time"
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