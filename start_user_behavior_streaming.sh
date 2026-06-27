#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_HOME="${SPARK_HOME:-/usr/local/spark-2.4.6}"
KAFKA_HOME="${KAFKA_HOME:-/usr/local/kafka_2.5.0}"
SCALA_BINARY_VERSION="${SCALA_BINARY_VERSION:-2.11}"
SPARK_VERSION="${SPARK_VERSION:-2.4.6}"
SPARK_KAFKA_PACKAGE="${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}:${SPARK_VERSION}}"
LOCAL_CONNECTOR_DIR="$ROOT_DIR/SparkMain/lib"
LOCAL_SPARK_KAFKA_JAR="$LOCAL_CONNECTOR_DIR/spark-sql-kafka-0-10_2.11-2.4.6.jar"
LOCAL_KAFKA_CLIENT_JAR="$LOCAL_CONNECTOR_DIR/kafka-clients-2.0.0.jar"
TOPIC="${KAFKA_USER_BEHAVIOR_TOPIC:-taobao_behavior}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
CHECKPOINT="${USER_BEHAVIOR_CHECKPOINT:-/tmp/spark/checkpoints/luckyanjun_user_behavior}"
MYSQL_HOST="${MYSQL_HOST:-localhost}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"
WEB_PORT="${REALTIME_WEB_PORT:-18080}"
JAR_PATH="${USER_BEHAVIOR_JAR:-$ROOT_DIR/SparkMain/target/userbehavior-realtime.jar}"

start_service() {
    local process_pattern="$1"
    local command="$2"
    local label="$3"
    if pgrep -f "$process_pattern" >/dev/null 2>&1; then
        echo "[INFO] $label is already running"
    else
        echo "[INFO] Starting $label"
        eval "$command"
        sleep 3
    fi
}

start_service \
    "org.apache.zookeeper.server.quorum.QuorumPeerMain" \
    "\"$KAFKA_HOME/bin/zookeeper-server-start.sh\" -daemon \"$KAFKA_HOME/config/zookeeper.properties\"" \
    "ZooKeeper"

start_service \
    "kafka.Kafka" \
    "\"$KAFKA_HOME/bin/kafka-server-start.sh\" -daemon \"$KAFKA_HOME/config/server.properties\"" \
    "Kafka"

broker_ready=false
for _ in $(seq 1 30); do
    if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
        --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
        broker_ready=true
        break
    fi
    sleep 1
done

if [ "$broker_ready" != "true" ]; then
    echo "[ERROR] Kafka broker did not become ready: $BOOTSTRAP"
    echo "[INFO] Inspect $KAFKA_HOME/logs/server.log"
    exit 1
fi

if "$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server "$BOOTSTRAP" \
    --list | grep -Fxq "$TOPIC"; then
    echo "[INFO] Kafka topic already exists: $TOPIC"
else
    "$KAFKA_HOME/bin/kafka-topics.sh" \
        --bootstrap-server "$BOOTSTRAP" \
        --create \
        --topic "$TOPIC" \
        --partitions 1 \
        --replication-factor 1
fi

MYSQL_ARGS=(-h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER")
if [ -n "$MYSQL_PASSWORD" ]; then
    MYSQL_ARGS+=("-p$MYSQL_PASSWORD")
fi
mysql "${MYSQL_ARGS[@]}" < "$ROOT_DIR/initializeSQL/02_luckyanjun_realtime_mysql.sql"

bash "$ROOT_DIR/build_user_behavior_realtime.sh"

mkdir -p "$ROOT_DIR/web/data"
if [ "${START_REALTIME_WEB:-true}" = "true" ]; then
    if pgrep -f "python3 -m http.server $WEB_PORT" >/dev/null 2>&1; then
        echo "[INFO] Realtime dashboard server is already running"
    elif ss -lnt | grep -Eq "[:.]${WEB_PORT}[[:space:]]"; then
        echo "[ERROR] Dashboard port is already in use: $WEB_PORT"
        exit 1
    else
        (
            cd "$ROOT_DIR/web"
            nohup python3 -m http.server "$WEB_PORT" \
                > /tmp/luckyanjun-realtime-web.log 2>&1 &
            echo $! > /tmp/luckyanjun-realtime-web.pid
        )
        sleep 1
        if ! curl -fsS "http://localhost:$WEB_PORT/luckyanjun_realtime.html" \
            >/dev/null; then
            echo "[ERROR] Realtime dashboard server failed to start"
            exit 1
        fi
    fi
    echo "[INFO] Dashboard: http://$(hostname -I | awk '{print $1}'):$WEB_PORT/luckyanjun_realtime.html"
fi

export MYSQL_JDBC_URL="${MYSQL_JDBC_URL:-jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/bigdata_ana?useUnicode=true&characterEncoding=utf8&useSSL=false}"
export MYSQL_USER
export MYSQL_PASSWORD
export REALTIME_SNAPSHOT_PATH="${REALTIME_SNAPSHOT_PATH:-$ROOT_DIR/web/data/realtime.json}"

echo "[INFO] Starting Spark Structured Streaming"
echo "[INFO] Kafka source: $BOOTSTRAP/$TOPIC"

if [ -f "$LOCAL_SPARK_KAFKA_JAR" ] && [ -f "$LOCAL_KAFKA_CLIENT_JAR" ]; then
    SPARK_DEPENDENCY_ARGS=(--jars "$LOCAL_SPARK_KAFKA_JAR,$LOCAL_KAFKA_CLIENT_JAR")
    echo "[INFO] Connector: vendored Spark 2.4.6 / Scala 2.11 jars"
else
    SPARK_DEPENDENCY_ARGS=(--packages "$SPARK_KAFKA_PACKAGE")
    echo "[INFO] Connector: $SPARK_KAFKA_PACKAGE"
fi

exec "$SPARK_HOME/bin/spark-submit" \
    --master "${SPARK_MASTER:-local[2]}" \
    "${SPARK_DEPENDENCY_ARGS[@]}" \
    --class org.bigdata.streaming.UserBehaviorRealtimeJob \
    "$JAR_PATH" \
    "$TOPIC" \
    "$BOOTSTRAP" \
    "$CHECKPOINT"
