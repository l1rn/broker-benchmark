#!/bin/bash

first_scenario () {
  BROKERS=("rabbitmq" "kafka")
  SIZES=(100 1024 10240 51200 102400)
  REPEATS=1

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
          --metrics-textfile="shared_metrics/${broker}_size${size}_run${r}.prom"
        
        sleep 10
      done
    done
  done
}

second_scanario () {
  SIZE=1024
  MODE=e2e
  PRODUCER_CONSUMER_COUNT=(1 2 4)
  MESSAGE_COUNT=100000
  BROKERS=("rabbitmq" "kafka")
  REPEATS=1
  for broker in ${BROKERS[@]}; do
    for count in ${PRODUCER_CONSUMER_COUNT[@]}; do
      for ((r=1; r<=REPEATS; r++)); do
        echo "[$(date)] Running 2st scenario: broker=$broker, consumer=$count, producer=$time, repeat=$r"
        ./bin/benchmark \
          --broker=$broker \
          --mode=$MODE \
          --size=$SIZE \
          --producer=$count \
          --consumer=$count \
          --metrics-textfile="shared_metrics/${broker}_producer${count}_consumer${count}_run${r}.prom"
      done
    done
  done
}

third_scenario (){

}

main () {
  read -p "Which scenario to run (1, 2, 3 or all)?: " response
  if [[ "$response" == "1" ]]; then
    first_scenario
  elif [[ "$response" == "2"]]; then
    second_scanario
  elif [[ "$response" == "3"]]; then

  else
    first_scenario
    second_scanario
    third_scenario
  fi

}

main