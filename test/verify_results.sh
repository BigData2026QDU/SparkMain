#!/bin/bash
set -euo pipefail

HIVE_DB="bigdata_ana_test"
PASS=0
FAIL=0

check_table_exists() {
    local table_name="$1"
    if hive -e "USE ${HIVE_DB}; SHOW TABLES;" | grep -q "^${table_name}$"; then
        echo "[PASS] Table exists: $table_name"
        PASS=$((PASS + 1))
    else
        echo "[FAIL] Table missing: $table_name"
        FAIL=$((FAIL + 1))
    fi
}

check_table_has_data() {
    local table_name="$1"
    local count
    count="$(hive -e "USE ${HIVE_DB}; SELECT COUNT(*) FROM ${table_name};" 2>/dev/null | tail -1)"
    if [ "$count" -gt 0 ] 2>/dev/null; then
        echo "[PASS] Table has data: $table_name ($count rows)"
        PASS=$((PASS + 1))
    else
        echo "[FAIL] Table has no data: $table_name"
        FAIL=$((FAIL + 1))
    fi
}

check_file_exists() {
    local file_path="$1"
    if [ -f "$file_path" ]; then
        echo "[PASS] File exists: $file_path"
        PASS=$((PASS + 1))
    else
        echo "[FAIL] File missing: $file_path"
        FAIL=$((FAIL + 1))
    fi
}

echo "=========================================="
echo "1. Check base tables"
echo "=========================================="
check_table_exists "movies"
check_table_exists "ratings"
check_table_exists "tags"
check_table_exists "links"
check_table_exists "user_behavior"
check_table_exists "dwd_user_behavior_clean"

echo ""
echo "=========================================="
echo "2. Check loaded data"
echo "=========================================="
check_table_has_data "movies"
check_table_has_data "ratings"
check_table_has_data "tags"
check_table_has_data "links"
check_table_has_data "user_behavior"
check_table_has_data "dwd_user_behavior_clean"

echo ""
echo "=========================================="
echo "3. Check views"
echo "=========================================="
if hive -e "USE ${HIVE_DB}; SHOW VIEWS;" | grep -q "v_movies_ratings"; then
    echo "[PASS] View exists: v_movies_ratings"
    PASS=$((PASS + 1))
else
    echo "[FAIL] View missing: v_movies_ratings"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "=========================================="
echo "4. Check analysis result tables"
echo "=========================================="
check_table_exists "task1_movie_stats"
check_table_has_data "task1_movie_stats"
check_table_exists "lb_funnel_item_path_stage"
check_table_has_data "lb_funnel_item_path_stage"
check_table_exists "lb_time_hour_distribution"
check_table_has_data "lb_time_hour_distribution"
check_table_exists "lb_category_topn"
check_table_has_data "lb_category_topn"
check_table_exists "lb_user_active_day_distribution"
check_table_has_data "lb_user_active_day_distribution"

echo ""
echo "=========================================="
echo "5. Check realtime analysis artifacts"
echo "=========================================="
check_file_exists "SparkMain/src/main/scala/org/bigdata/streaming/UserBehaviorRealtimeJob.scala"
check_file_exists "SparkMain/src/main/scala/org/bigdata/streaming/UserBehaviorReplayProducer.scala"
check_file_exists "initializeSQL/02_luckyanjun_realtime_mysql.sql"
check_file_exists "web/luckyanjun_realtime.html"

echo ""
echo "=========================================="
echo "Verification result"
echo "=========================================="
echo "PASS: $PASS"
echo "FAIL: $FAIL"

if [ "$FAIL" -eq 0 ]; then
    echo "[SUCCESS] All verification checks passed"
    exit 0
fi

echo "[ERROR] Verification failed"
exit 1
