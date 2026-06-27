#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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

export MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
export MYSQL_PORT="${MYSQL_PORT:-13306}"
export MYSQL_USER="${MYSQL_USER:-test}"
export MYSQL_PASSWORD="$REMOTE_MYSQL_PASSWORD"
export MYSQL_DATABASE="${MYSQL_DATABASE:-test_db}"
export MYSQL_CREATE_DATABASE=false
export KAFKA_USER_BEHAVIOR_TOPIC="${KAFKA_USER_BEHAVIOR_TOPIC:-taobao_behavior}"
export KAFKA_STARTING_OFFSETS="${KAFKA_STARTING_OFFSETS:-latest}"
export USER_BEHAVIOR_CHECKPOINT="${USER_BEHAVIOR_CHECKPOINT:-/tmp/spark/checkpoints/luckyanjun_user_behavior_remote}"
export REALTIME_WEB_PORT="${REALTIME_WEB_PORT:-18080}"

exec bash "$ROOT_DIR/start_user_behavior_streaming.sh"
