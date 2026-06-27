#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_HOME="${SPARK_HOME:-/usr/local/spark-2.4.6}"
SCALA_HOME="${SCALA_HOME:-/usr/local/scala-2.11.12}"
KAFKA_HOME="${KAFKA_HOME:-/usr/local/kafka_2.5.0}"
TARGET_DIR="$ROOT_DIR/SparkMain/target"
CLASSES_DIR="$TARGET_DIR/userbehavior-realtime-classes"
JAR_PATH="${USER_BEHAVIOR_JAR:-$TARGET_DIR/userbehavior-realtime.jar}"

for path in "$SPARK_HOME" "$SCALA_HOME" "$KAFKA_HOME"; do
    if [ ! -d "$path" ]; then
        echo "[ERROR] Required runtime directory does not exist: $path"
        exit 1
    fi
done

rm -rf "$CLASSES_DIR"
mkdir -p "$CLASSES_DIR"

SPARK_CP="$(printf "%s:" "$SPARK_HOME"/jars/*.jar)"
KAFKA_CP="$(printf "%s:" "$KAFKA_HOME"/libs/*.jar)"

"$SCALA_HOME/bin/scalac" \
    -target:jvm-1.8 \
    -classpath "$SPARK_CP$KAFKA_CP" \
    -d "$CLASSES_DIR" \
    "$ROOT_DIR/SparkMain/src/main/scala/org/bigdata/streaming/UserBehaviorRealtimeJob.scala" \
    "$ROOT_DIR/SparkMain/src/main/scala/org/bigdata/streaming/UserBehaviorReplayProducer.scala" \
    "$ROOT_DIR/SparkMain/src/main/scala/org/bigdata/export/UserBehaviorMySQLExportJob.scala"

(
    cd "$CLASSES_DIR"
    jar cf "$JAR_PATH" org/bigdata/streaming/*.class org/bigdata/export/*.class
)

echo "[SUCCESS] Built $JAR_PATH"
