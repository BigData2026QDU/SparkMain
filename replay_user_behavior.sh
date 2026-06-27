#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCALA_HOME="${SCALA_HOME:-/usr/local/scala-2.11.12}"
KAFKA_HOME="${KAFKA_HOME:-/usr/local/kafka_2.5.0}"
JAR_PATH="${USER_BEHAVIOR_JAR:-$ROOT_DIR/SparkMain/target/userbehavior-realtime.jar}"
INPUT="${1:-$ROOT_DIR/dataset_test/UserBehavior.csv}"
TOPIC="${2:-${KAFKA_USER_BEHAVIOR_TOPIC:-taobao_behavior}}"
BOOTSTRAP="${3:-${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}}"
DELAY_MS="${4:-200}"
MAX_ROWS="${5:-0}"

if [ ! -f "$JAR_PATH" ]; then
    bash "$ROOT_DIR/build_user_behavior_realtime.sh"
fi

exec java \
    -cp "$JAR_PATH:$SCALA_HOME/lib/scala-library.jar:$KAFKA_HOME/libs/*" \
    org.bigdata.streaming.UserBehaviorReplayProducer \
    "$INPUT" \
    "$TOPIC" \
    "$BOOTSTRAP" \
    "$DELAY_MS" \
    "$MAX_ROWS"
