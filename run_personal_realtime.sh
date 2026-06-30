#!/bin/bash

set -euo pipefail

SPARK_MASTER="${SPARK_MASTER:-local[*]}"
JAR_PATH="${JAR_PATH:-SparkMain/target/scala-2.12/sparkmain_2.12-1.0.0.jar}"
SPARK_SUBMIT_JARS="${SPARK_SUBMIT_JARS:-}"
SPARK_SUBMIT_PACKAGES="${SPARK_SUBMIT_PACKAGES:-}"
MYSQL_CLEAN_TABLES="${MYSQL_CLEAN_TABLES:-false}"

export REALTIME_SOURCE="${REALTIME_SOURCE:-file}"
export REALTIME_INPUT_PATH="${REALTIME_INPUT_PATH:-dataset_test/realtime_ratings}"
export REALTIME_OUTPUT_PATH="${REALTIME_OUTPUT_PATH:-output/personal_realtime}"
export REALTIME_CHECKPOINT_PATH="${REALTIME_CHECKPOINT_PATH:-output/checkpoints/personal_realtime_ratings}"
if [ -z "${REALTIME_TRIGGER_ONCE:-}" ]; then
  if [ "$REALTIME_SOURCE" = "kafka" ]; then
    export REALTIME_TRIGGER_ONCE=false
  else
    export REALTIME_TRIGGER_ONCE=true
  fi
fi
export REALTIME_TRIGGER_INTERVAL="${REALTIME_TRIGGER_INTERVAL:-2 seconds}"
export MYSQL_ENABLED="${MYSQL_ENABLED:-false}"

if ! command -v spark-submit >/dev/null 2>&1; then
  echo "[ERROR] spark-submit not found"
  exit 1
fi

if [ ! -f "$JAR_PATH" ]; then
  echo "[ERROR] JAR not found: $JAR_PATH"
  echo "[HINT] Build the project first, or set JAR_PATH to an existing jar."
  exit 1
fi

validate_output_path() {
  local name=$1
  local path=$2

  if [ -z "$path" ] || [ "$path" = "/" ] || [ "$path" = "." ] || [ "$path" = ".." ]; then
    echo "[ERROR] Refusing unsafe $name: $path"
    exit 1
  fi

  case "$path" in
    output/*) ;;
    *)
      echo "[ERROR] $name must be a relative path under output/: $path"
      exit 1
      ;;
  esac

  case "$path" in
    *".."*|/*)
      echo "[ERROR] Refusing unsafe $name: $path"
      exit 1
      ;;
  esac
}

clean_mysql_tables_if_requested() {
  if [ "$MYSQL_CLEAN_TABLES" != "true" ]; then
    return
  fi

  if [ "${MYSQL_ENABLED:-false}" != "true" ]; then
    echo "[ERROR] MYSQL_CLEAN_TABLES=true requires MYSQL_ENABLED=true"
    exit 1
  fi

  if ! command -v mysql >/dev/null 2>&1; then
    echo "[ERROR] MYSQL_CLEAN_TABLES=true requires mysql client"
    exit 1
  fi

  local jdbc_url="${MYSQL_JDBC_URL:-}"
  local mysql_user="${MYSQL_USER:-}"
  local mysql_password="${MYSQL_PASSWORD:-}"
  local metrics_table="${MYSQL_REALTIME_METRICS_TABLE:-realtime_rating_metrics}"
  local top_movies_table="${MYSQL_REALTIME_TOP_MOVIES_TABLE:-realtime_top_movies}"
  local alerts_table="${MYSQL_REALTIME_ALERTS_TABLE:-realtime_rating_alerts}"
  local overview_table="${MYSQL_REALTIME_OVERVIEW_TABLE:-yc_realtime_overview}"

  validate_table_name "$metrics_table"
  validate_table_name "$top_movies_table"
  validate_table_name "$alerts_table"
  validate_table_name "$overview_table"

  if [ -z "$jdbc_url" ] || [ -z "$mysql_user" ]; then
    echo "[ERROR] MYSQL_CLEAN_TABLES=true requires MYSQL_JDBC_URL and MYSQL_USER"
    exit 1
  fi

  local host_port db_name host port
  host_port=$(printf '%s' "$jdbc_url" | sed -n 's#^jdbc:mysql://\([^/]*\)/.*#\1#p')
  db_name=$(printf '%s' "$jdbc_url" | sed -n 's#^jdbc:mysql://[^/]*/\([^?]*\).*#\1#p')
  host=${host_port%%:*}
  port=${host_port##*:}
  if [ "$host" = "$port" ]; then
    port=3306
  fi

  if [ -z "$host" ] || [ -z "$db_name" ]; then
    echo "[ERROR] Cannot parse MYSQL_JDBC_URL for cleanup: $jdbc_url"
    exit 1
  fi

  echo "[INFO] Cleaning realtime MySQL tables in $db_name"
  MYSQL_PWD="$mysql_password" mysql \
    -h "$host" \
    -P "$port" \
    -u "$mysql_user" \
    "$db_name" \
    -e "DROP TABLE IF EXISTS \`$metrics_table\`, \`$top_movies_table\`, \`$alerts_table\`, \`$overview_table\`;"
}

validate_table_name() {
  local table=$1
  case "$table" in
    ''|*[!A-Za-z0-9_]*)
      echo "[ERROR] Unsafe MySQL table name: $table"
      exit 1
      ;;
  esac
}

validate_output_path "REALTIME_OUTPUT_PATH" "$REALTIME_OUTPUT_PATH"
validate_output_path "REALTIME_CHECKPOINT_PATH" "$REALTIME_CHECKPOINT_PATH"
clean_mysql_tables_if_requested

rm -rf "$REALTIME_OUTPUT_PATH" "$REALTIME_CHECKPOINT_PATH"
mkdir -p "$REALTIME_OUTPUT_PATH"

echo "[INFO] Running personal realtime rating analysis"
echo "[INFO] REALTIME_SOURCE=$REALTIME_SOURCE"
echo "[INFO] REALTIME_INPUT_PATH=$REALTIME_INPUT_PATH"
echo "[INFO] REALTIME_OUTPUT_PATH=$REALTIME_OUTPUT_PATH"
echo "[INFO] REALTIME_TRIGGER_ONCE=$REALTIME_TRIGGER_ONCE"
echo "[INFO] REALTIME_TRIGGER_INTERVAL=$REALTIME_TRIGGER_INTERVAL"
if [ "$REALTIME_SOURCE" = "kafka" ]; then
  echo "[INFO] KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
  echo "[INFO] KAFKA_TOPIC=${KAFKA_TOPIC:-ratings_personal_realtime}"
fi

spark_args=(--class org.bigdata.Main --master "$SPARK_MASTER")

if [ -n "$SPARK_SUBMIT_JARS" ]; then
  spark_args+=(--jars "$SPARK_SUBMIT_JARS")
fi

if [ -n "$SPARK_SUBMIT_PACKAGES" ]; then
  spark_args+=(--packages "$SPARK_SUBMIT_PACKAGES")
fi

spark-submit "${spark_args[@]}" "$JAR_PATH" realtime

echo "[INFO] Personal realtime rating analysis finished"
