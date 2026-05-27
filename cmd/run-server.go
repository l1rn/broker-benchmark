package main

import (
	"net/http"
	"os"
)

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	data, err := os.ReadFile("./shared_metrics/kafka-e2e.prom")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write(data)
}

func main() {
	http.HandleFunc("/metrics", metricsHandler)

	http.ListenAndServe("0.0.0.0:8081", nil)
}