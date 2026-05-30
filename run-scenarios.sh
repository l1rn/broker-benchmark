#!/bin/bash
BROKERS=("rabbitmq" "kafka")
REPEATS=1

first_scenario () {
  SIZES=(100 1024 10240 51200 102400)

  for broker in "${BROKERS[@]}"; do
    for size in "${SIZES[@]}"; do
      for ((r=1; r<=REPEATS; r++)); do
        echo "[$(date)] Running 1st scenario: broker=$broker, size=$size, repeat=$r"
        ./bin/benchmark \
          --broker="$broker" \
          --mode=e2e \
          --size="$size" \
          --count=10000 \
          --producers=1 \
          --consumers=1 \
          --metrics-textfile="shared_metrics/sc1/${broker}_size${size}_run${r}.prom"
        
        sleep 10
      done
    done
  done
}

second_scenario () {
  SIZE=1024
  MODE="e2e"
  CLIENT_COUNT=(1 2 4)
  MESSAGE_COUNT=100000
  for broker in ${BROKERS[@]}; do
    for count in ${CLIENT_COUNT[@]}; do
      for ((r=1; r<=REPEATS; r++)); do
        echo "[$(date)] Running 2nd scenario: broker=$broker, consumer=$count, producer=$time, repeat=$r"
        ./bin/benchmark \
          --broker=$broker \
          --mode=$MODE \
          --size=$SIZE \
          --producers=$count \
          --consumers=$count \
          --count=$MESSAGE_COUNT \
          --metrics-textfile="shared_metrics/sc2/${broker}_producer${count}_consumer${count}_run${r}.prom"
        sleep 10
      done
    done
  done
}

third_scenario (){
  MODE=e2e
  DELIVERY_MODES=("persistent" "transient")
  CLIENT_COUNT=4
  MESSAGE_COUNT=50000
  REPEATS=1
  for broker in ${BROKERS[@]}; do
    for deliveryMode in ${DELIVERY_MODES[@]}; do
      for ((r=1;r<=REPEATS; r++)); do
      echo "[$(date)] Running 3rd scenario: broker=$broker, consumer=$CLIENT_COUNT, producer=$CLIENT_COUNT, repeat=$r"
      ./bin/benchmark \
        --broker=$broker \
        --mode=$MODE \
        --delivery-mode=$deliveryMode \
        --size=1024 \
        --count=$MESSAGE_COUNT \
        --consumers=$CLIENT_COUNT \
        --producers=$CLIENT_COUNT \
        --metrics-textfile="shared_metrics/sc3/${broker}_producer${CLIENT_COUNT}_consumer${CLIENT_COUNT}_delivery-${deliveryMode}_run${r}.prom"
      done
    done
  done
}

fourth_scenario() {
  SIZE=1024
  CLIENT_COUNT=(1 4 16)
  MESSAGE_COUNT=50000
  
  for broker in ${BROKERS[@]}; do
    for count in ${CLIENT_COUNT[@]}; do
      echo "[$(date)] Pre-populating for consumer test: broker=$broker, messages=$MESSAGE_COUNT"
      ./bin/benchmark \
        --broker=$broker \
        --size=$SIZE \
        --mode=producer \
        --count=$MESSAGE_COUNT \
        --producers=4 \
        --metrics-textfile="shared_metrics/sc4/${broker}_producer_prepopulate_run${count}.prom"

      sleep 5
      echo "[$(date)] Running 5th scenario (CONSUMER): broker=$broker, consumers=$count"
      ./bin/benchmark \
        --broker=$broker \
        --size=$SIZE \
        --mode=consumer \
        --count=$MESSAGE_COUNT \
        --consumers=$count \
        --metrics-textfile="shared_metrics/sc4/${broker}_consumer-only${count}.prom"
      sleep 5
    done
  done
}

main () {
  echo "Available scenarios:"
  echo "  1 - Message size impact"
  echo "  2 - Parallelism impact"
  echo "  3 - Persistent vs Transient"
  echo "  4 - Producer vs Consumer isolation"
  echo "  all - Run all scenarios"
  read -p "Which scenario to run (1, 2, 3, 4 or all)?: " response
  
  case "$response" in
    "1") first_scenario;;
    "2") second_scenario;;
	  "3") third_scenario;;
    "4") fourth_scenario;;
    "all")
      first_scenario
      second_scenario
      third_scenario
      fourth_scenario
      ;;
    "*") echo "Invalid choice. Available only: {1|2|3|4|all}"
  esac
}

main
