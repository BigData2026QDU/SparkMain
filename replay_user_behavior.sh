#!/bin/bash
set -e

INPUT="${1:-../dataset_test/UserBehavior.csv}"
TOPIC="${2:-taobao_behavior}"
BOOTSTRAP="${3:-localhost:9092}"
DELAY_MS="${4:-200}"

cd SparkMain
mvn -q -DskipTests package
java -cp target/spark-streaming-kafka-1.0.0.jar org.example.streaming.UserBehaviorProducer "$INPUT" "$TOPIC" "$BOOTSTRAP" "$DELAY_MS"
