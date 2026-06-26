#!/bin/bash

################################################################################
# Kafka + Spark Streaming 启动脚本
################################################################################

set -e

echo "=========================================="
echo "启动 Kafka + Spark Streaming 环境"
echo "=========================================="

# 检查 Kafka 是否安装
if ! command -v kafka-server-start.sh &> /dev/null; then
    echo "[错误] Kafka 未安装，请先安装 Kafka"
    exit 1
fi

# 检查 Spark 是否安装
if ! command -v spark-submit &> /dev/null; then
    echo "[错误] Spark 未安装，请先安装 Spark"
    exit 1
fi

# 启动 ZooKeeper
echo "[步骤1] 启动 ZooKeeper..."
if pgrep -f "zookeeper" > /dev/null; then
    echo "[信息] ZooKeeper 已运行"
else
    zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
    sleep 3
    echo "[成功] ZooKeeper 启动成功"
fi

# 启动 Kafka
echo "[步骤2] 启动 Kafka..."
if pgrep -f "kafka.server.Kafka" > /dev/null; then
    echo "[信息] Kafka 已运行"
else
    kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
    sleep 5
    echo "[成功] Kafka 启动成功"
fi

# 创建 Topic
echo "[步骤3] 创建 Kafka Topic..."
kafka-topics.sh --create --topic ratings --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
echo "[成功] Topic 创建成功"

# 启动 Spark Streaming 应用
echo "[步骤4] 启动 Spark Streaming 应用..."
spark-submit \
    --class org.example.streaming.RatingStreamProcessor \
    --master local[*] \
    SparkMain/target/spark-streaming-kafka-1.0.0.jar &

sleep 5
echo "[成功] Spark Streaming 应动启动成功"

echo ""
echo "=========================================="
echo "环境启动完成"
echo "=========================================="
echo ""
echo "启动数据生成器:"
echo "  java -cp SparkMain/target/spark-streaming-kafka-1.0.0.jar org.example.streaming.RatingProducer"
echo ""
echo "停止服务:"
echo "  kafka-server-stop.sh"
echo "  zookeeper-server-stop.sh"
