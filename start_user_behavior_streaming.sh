#!/bin/bash
set -e

TOPIC="${KAFKA_USER_BEHAVIOR_TOPIC:-taobao_behavior}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

echo "Starting LuckyAnJun UserBehavior streaming processor"
echo "Topic: $TOPIC"
echo "Bootstrap servers: $BOOTSTRAP"

cd SparkMain
mvn -q -DskipTests package

spark-submit \
  --class org.example.streaming.UserBehaviorStreamProcessor \
  --master "${SPARK_MASTER:-local[*]}" \
  target/spark-streaming-kafka-1.0.0.jar \
  "$TOPIC" "$BOOTSTRAP"
