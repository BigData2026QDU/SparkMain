#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_HOME="${SPARK_HOME:-/usr/local/spark-2.4.6}"
SOURCE_DATABASE="${SOURCE_DATABASE:-bigdata_ana}"
MYSQL_TUNNEL_HOST="${MYSQL_TUNNEL_HOST:-127.0.0.1}"
MYSQL_TUNNEL_PORT="${MYSQL_TUNNEL_PORT:-13306}"
MYSQL_DATABASE="${MYSQL_DATABASE:-test_db}"
MYSQL_USER="${MYSQL_USER:-test}"
JAR_PATH="${USER_BEHAVIOR_JAR:-$ROOT_DIR/SparkMain/target/userbehavior-realtime.jar}"
CREDENTIAL_FILE="${MYSQL_CREDENTIAL_FILE:-/home/master/.sparkmain_mysql.env}"

if [ ! -f "$CREDENTIAL_FILE" ]; then
    echo "[ERROR] MySQL credential file not found: $CREDENTIAL_FILE"
    exit 1
fi

set -a
source "$CREDENTIAL_FILE"
set +a

if [ -z "${REMOTE_MYSQL_PASSWORD:-}" ]; then
    echo "[ERROR] REMOTE_MYSQL_PASSWORD is not set"
    exit 1
fi

bash "$ROOT_DIR/build_user_behavior_realtime.sh"

export MYSQL_USER
export MYSQL_PASSWORD="$REMOTE_MYSQL_PASSWORD"
export MYSQL_JDBC_URL="${MYSQL_JDBC_URL:-jdbc:mysql://${MYSQL_TUNNEL_HOST}:${MYSQL_TUNNEL_PORT}/${MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&rewriteBatchedStatements=true}"

exec "$SPARK_HOME/bin/spark-submit" \
    --master "${SPARK_MASTER:-local[2]}" \
    --class org.bigdata.export.UserBehaviorMySQLExportJob \
    "$JAR_PATH" \
    "$SOURCE_DATABASE" \
    "$MYSQL_JDBC_URL" \
    "$MYSQL_USER"
