#!/bin/bash

set -u

PASS=0
FAIL=0

check_file_exists() {
    local file=$1
    if [ -f "$file" ]; then
        echo "[PASS] file exists: $file"
        PASS=$((PASS + 1))
    else
        echo "[FAIL] file missing: $file"
        FAIL=$((FAIL + 1))
    fi
}

check_directory_exists() {
    local dir=$1
    if [ -d "$dir" ]; then
        echo "[PASS] directory exists: $dir"
        PASS=$((PASS + 1))
    else
        echo "[FAIL] directory missing: $dir"
        FAIL=$((FAIL + 1))
    fi
}

check_path_absent() {
    local path=$1
    if [ ! -e "$path" ]; then
        echo "[PASS] path absent: $path"
        PASS=$((PASS + 1))
    else
        echo "[FAIL] path should not exist: $path"
        FAIL=$((FAIL + 1))
    fi
}

echo "=========================================="
echo "1. Project structure"
echo "=========================================="

check_directory_exists "SparkMain/src/main/scala/org/bigdata"
check_file_exists "SparkMain/build.sbt"
check_file_exists "main_pipeline_new.sh"
check_file_exists "main_pipeline_test.sh"
check_file_exists "run_personal_realtime.sh"

echo ""
echo "=========================================="
echo "2. Personal analysis tasks"
echo "=========================================="

check_file_exists "SparkMain/src/main/scala/org/bigdata/Main.scala"
check_file_exists "SparkMain/src/main/scala/org/bigdata/analysis/AnalyzeRatings.scala"
check_file_exists "SparkMain/src/main/scala/org/bigdata/analysis/AnalyzeGenres.scala"
check_file_exists "SparkMain/src/main/scala/org/bigdata/analysis/AnalyzeTime.scala"
check_file_exists "SparkMain/src/main/scala/org/bigdata/analysis/AnalyzeUsers.scala"
check_file_exists "SparkMain/src/main/scala/org/bigdata/streaming/PersonalRealtimeRatings.scala"

echo ""
echo "=========================================="
echo "3. Realtime test input"
echo "=========================================="

check_directory_exists "dataset_test/realtime_ratings"
check_file_exists "dataset_test/realtime_ratings/ratings_batch_1.csv"

echo ""
echo "=========================================="
echo "4. MySQL export support"
echo "=========================================="

check_file_exists "SparkMain/src/main/scala/org/bigdata/utils/MySQLExporter.scala"

echo ""
echo "=========================================="
echo "5. Personal branch boundary"
echo "=========================================="

check_path_absent "config/streaming.properties"

echo ""
echo "=========================================="
echo "Verification result"
echo "=========================================="
echo "Passed: $PASS"
echo "Failed: $FAIL"

if [ "$FAIL" -eq 0 ]; then
    exit 0
fi

exit 1
