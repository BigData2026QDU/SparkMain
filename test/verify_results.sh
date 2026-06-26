#!/usr/bin/env bash

set -euo pipefail

SPARK_SUBMIT_CMD="${SPARK_SUBMIT_CMD:-spark-submit}"
DB_NAME="${DB_NAME:-bigdata_ana_test}"
DATASET_DIR="${DATASET_DIR:-dataset_test}"
CLEANED_DIR="${CLEANED_DIR:-cleanedDataset_test}"
PREPARE_DATA_DIR="${PREPARE_DATA_DIR:-prepareData_test}"
JOB_SQL_DIR="${JOB_SQL_DIR:-jobSQL_test}"
WAREHOUSE_DIR="${WAREHOUSE_DIR:-spark-warehouse-verify}"
OUTPUT_DIR="${OUTPUT_DIR:-output/verify}"
JAR_PATH="${JAR_PATH:-SparkMain/target/spark-streaming-kafka-1.0.0.jar}"

if ! command -v "$SPARK_SUBMIT_CMD" >/dev/null 2>&1; then
    printf '[ERROR] Command not found: %s\n' "$SPARK_SUBMIT_CMD"
    exit 1
fi

printf '\nSparkMain Spark SQL result verification\n'
printf 'Database: %s\n' "$DB_NAME"

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
