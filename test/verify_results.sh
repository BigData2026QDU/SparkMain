#!/usr/bin/env bash

set -euo pipefail

HIVE_DB="${HIVE_DB:-bigdata_ana_test}"
HIVE_CMD="${HIVE_CMD:-hive}"

pass_count=0
fail_count=0

run_hive_scalar() {
    local sql="$1"
    "$HIVE_CMD" -S -e "$sql" 2>/dev/null | tail -n 1 | tr -d '\r'
}

record_pass() {
    printf '[PASS] %s\n' "$1"
    pass_count=$((pass_count + 1))
}

record_fail() {
    printf '[FAIL] %s\n' "$1"
    fail_count=$((fail_count + 1))
}

check_table_exists() {
    local table_name="$1"
    if "$HIVE_CMD" -S -e "USE ${HIVE_DB}; SHOW TABLES LIKE '${table_name}';" 2>/dev/null | grep -qx "$table_name"; then
        record_pass "table ${table_name} exists"
    else
        record_fail "table ${table_name} is missing"
    fi
}

check_table_has_data() {
    local table_name="$1"
    local row_count
    row_count="$(run_hive_scalar "USE ${HIVE_DB}; SELECT COUNT(*) FROM ${table_name};")"
    if [[ "$row_count" =~ ^[0-9]+$ ]] && [ "$row_count" -gt 0 ]; then
        record_pass "table ${table_name} has ${row_count} rows"
    else
        record_fail "table ${table_name} has no data"
    fi
}

printf '\n'
printf '========================================\n'
printf 'SparkMain Hive result verification\n'
printf 'Database: %s\n' "$HIVE_DB"
printf '========================================\n'

for table in movies ratings tags links task1_movie_stats task2_movie_ranking task3_genre_stats; do
    check_table_exists "$table"
done

for table in movies ratings tags links task1_movie_stats task2_movie_ranking task3_genre_stats; do
    check_table_has_data "$table"
done

if "$HIVE_CMD" -S -e "USE ${HIVE_DB}; SHOW VIEWS LIKE 'v_movies_ratings';" 2>/dev/null | grep -qx "v_movies_ratings"; then
    record_pass "view v_movies_ratings exists"
else
    record_fail "view v_movies_ratings is missing"
fi

printf '\nPassed: %s\nFailed: %s\n' "$pass_count" "$fail_count"

if [ "$fail_count" -ne 0 ]; then
    exit 1
fi
