#!/bin/bash

BROKERS=("rabbitmq" "kafka")
SIZES=(100 1024 10240 51200 102400)
REPEATS=1

for broker in "${BROKERS[@]}"; do
  for size in "${SIZES[@]}"; do
    for ((r=1; r<=REPEATS; r++)); do
      echo "[$(date)] Running: broker=$broker, size=$size, repeat=$r"
      ./bin/benchmark \
        --broker="$broker" \
        --mode=e2e \
        --size="$size" \
        --count=10000 \
        --producers=1 \
        --consumers=1 \
        --metrics-textfile="shared_metrics/${broker}_size${size}_run${r}.prom"
      
      sleep 10
    done
  done
done

echo "[$(date)] Scenario 1 completed."