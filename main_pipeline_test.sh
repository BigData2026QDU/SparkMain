#!/usr/bin/env bash

set -euo pipefail

SPARK_SUBMIT_CMD="${SPARK_SUBMIT_CMD:-spark-submit}"
MVN_CMD="${MVN_CMD:-mvn}"
PYTHON_CMD="${PYTHON_CMD:-python3}"

DB_NAME="${DB_NAME:-bigdata_ana_test}"
DATASET_DIR="${DATASET_DIR:-dataset_test}"
CLEANED_DIR="${CLEANED_DIR:-cleanedDataset_test}"
CLEANPY_DIR="${CLEANPY_DIR:-cleanPy_test}"
PREPARE_DATA_DIR="${PREPARE_DATA_DIR:-prepareData_test}"
JOB_SQL_DIR="${JOB_SQL_DIR:-jobSQL_test}"
WAREHOUSE_DIR="${WAREHOUSE_DIR:-spark-warehouse-test}"
OUTPUT_DIR="${OUTPUT_DIR:-output/bigdata_ana_test}"
JAR_PATH="${JAR_PATH:-SparkMain/target/spark-streaming-kafka-1.0.0.jar}"

print_step() {
    printf '\n================================================================================\n'
    printf 'Step %s: %s\n' "$1" "$2"
    printf '================================================================================\n'
}

check_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        printf '[ERROR] Command not found: %s\n' "$1"
        exit 1
    fi
}

check_directory() {
    if [ ! -d "$1" ]; then
        printf '[ERROR] Directory not found: %s\n' "$1"
        exit 1
    fi
}

printf '\nSparkMain lightweight Spark SQL pipeline test\n'

print_step 1 "Check local tools and directories"
check_command "$SPARK_SUBMIT_CMD"
check_command "$MVN_CMD"
check_command "$PYTHON_CMD"
check_directory "$DATASET_DIR"
check_directory "$CLEANPY_DIR"
check_directory "$PREPARE_DATA_DIR"
check_directory "$JOB_SQL_DIR"

print_step 2 "Prepare cleaned test data"
rm -rf "$CLEANED_DIR"
mkdir -p "$CLEANED_DIR"
while IFS= read -r py_file; do
    printf '[INFO] Running cleaner: %s\n' "$py_file"
    "$PYTHON_CMD" "$py_file"
done < <(find "$CLEANPY_DIR" -name "*.py" | sort)

print_step 3 "Build Spark Scala pipeline"
"$MVN_CMD" -B -f SparkMain/pom.xml -DskipTests package

print_step 4 "Run Spark SQL analysis"
rm -rf "$WAREHOUSE_DIR" "$OUTPUT_DIR"
"$SPARK_SUBMIT_CMD" \
    --class org.example.pipeline.SparkSqlPipeline \
    --master local[2] \
    "$JAR_PATH" \
    --database "$DB_NAME" \
    --dataset-dir "$DATASET_DIR" \
    --cleaned-dir "$CLEANED_DIR" \
    --prepare-dir "$PREPARE_DATA_DIR" \
    --job-dir "$JOB_SQL_DIR" \
    --warehouse-dir "$WAREHOUSE_DIR" \
    --output-dir "$OUTPUT_DIR" \
    --validate

printf '\n[SUCCESS] Lightweight Spark SQL pipeline test completed\n'
