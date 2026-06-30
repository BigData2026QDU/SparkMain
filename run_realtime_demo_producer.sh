#!/bin/bash

set -euo pipefail

KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-ratings_personal_realtime}"
REALTIME_SOURCE_CSV="${REALTIME_SOURCE_CSV:-dataset/ratings.csv}"
DEMO_EVENTS_PER_WINDOW="${DEMO_EVENTS_PER_WINDOW:-80}"
DEMO_SLEEP_SECONDS="${DEMO_SLEEP_SECONDS:-4}"
DEMO_WINDOW_SECONDS="${DEMO_WINDOW_SECONDS:-300}"

find_kafka_cmd() {
  local cmd=$1
  if command -v "$cmd" >/dev/null 2>&1; then
    command -v "$cmd"
    return
  fi

  for dir in "${KAFKA_HOME:-}/bin" /usr/local/kafka/bin /opt/kafka/bin "$HOME/kafka/bin"; do
    if [ -n "$dir" ] && [ -x "$dir/$cmd" ]; then
      printf '%s\n' "$dir/$cmd"
      return
    fi
  done

  echo "[ERROR] Cannot find $cmd. Set KAFKA_HOME or add Kafka bin to PATH." >&2
  exit 1
}

KAFKA_PRODUCER="$(find_kafka_cmd kafka-console-producer.sh)"
if "$KAFKA_PRODUCER" --help 2>&1 | grep -q -- "--bootstrap-server"; then
  PRODUCER_CONNECTION_OPTION="--bootstrap-server"
else
  PRODUCER_CONNECTION_OPTION="--broker-list"
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "[ERROR] python3 not found"
  exit 1
fi

if [ ! -f "$REALTIME_SOURCE_CSV" ]; then
  echo "[ERROR] REALTIME_SOURCE_CSV not found: $REALTIME_SOURCE_CSV"
  exit 1
fi

echo "[INFO] Starting continuous realtime demo producer"
echo "[INFO] Kafka: $KAFKA_BOOTSTRAP_SERVERS topic=$KAFKA_TOPIC"
echo "[INFO] Producer option: $PRODUCER_CONNECTION_OPTION"
echo "[INFO] Source CSV: $REALTIME_SOURCE_CSV"
echo "[INFO] Every ${DEMO_SLEEP_SECONDS}s advances ${DEMO_WINDOW_SECONDS}s event time"

python3 scripts/continuous_kafka_replay.py \
  --csv "$REALTIME_SOURCE_CSV" \
  --producer "$KAFKA_PRODUCER" \
  --producer-connection-option="$PRODUCER_CONNECTION_OPTION" \
  --bootstrap-servers "$KAFKA_BOOTSTRAP_SERVERS" \
  --topic "$KAFKA_TOPIC" \
  --events-per-window "$DEMO_EVENTS_PER_WINDOW" \
  --sleep-seconds "$DEMO_SLEEP_SECONDS" \
  --window-seconds "$DEMO_WINDOW_SECONDS"
