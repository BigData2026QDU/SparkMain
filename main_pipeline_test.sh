#!/usr/bin/env bash

set -euo pipefail

HIVE_DB="${HIVE_DB:-bigdata_ana_test}"
HDFS_BASE_PATH="${HDFS_BASE_PATH:-/user/hive/bigdata_ana_test}"
HDFS_STAGE_PATH="${HDFS_STAGE_PATH:-${HDFS_BASE_PATH}/stage}"
PYTHON_CMD="${PYTHON_CMD:-python3}"
HIVE_CMD="${HIVE_CMD:-hive}"
HDFS_CMD="${HDFS_CMD:-hdfs}"

DATASET_DIR="${DATASET_DIR:-dataset_test}"
CLEANED_DIR="${CLEANED_DIR:-cleanedDataset_test}"
CLEANPY_DIR="${CLEANPY_DIR:-cleanPy_test}"
INITIALIZE_SQL_DIR="${INITIALIZE_SQL_DIR:-initializeSQL_test}"
PREPARE_DATA_DIR="${PREPARE_DATA_DIR:-prepareData_test}"
JOB_SQL_DIR="${JOB_SQL_DIR:-jobSQL_test}"

print_separator() {
    printf '%s\n' '================================================================================'
}

print_step() {
    local step_num="$1"
    local step_desc="$2"
    printf '\n'
    print_separator
    printf 'Step %s: %s\n' "$step_num" "$step_desc"
    print_separator
}

check_command() {
    local cmd="$1"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        printf '[ERROR] Command not found: %s\n' "$cmd"
        exit 1
    fi
}

check_directory() {
    local dir="$1"
    if [ ! -d "$dir" ]; then
        printf '[ERROR] Directory not found: %s\n' "$dir"
        exit 1
    fi
}

run_hive_file() {
    local sql_file="$1"
    printf '[INFO] Running Hive SQL: %s\n' "$sql_file"
    "$HIVE_CMD" -f "$sql_file"
}

stage_csv() {
    local local_file="$1"
    local hdfs_file="$2"
    printf '[INFO] Staging %s to %s\n' "$local_file" "$hdfs_file"
    "$HDFS_CMD" dfs -put -f "$local_file" "$hdfs_file"
}

load_table() {
    local table="$1"
    local hdfs_file="$2"
    printf '[INFO] Loading %s from %s\n' "$table" "$hdfs_file"
    "$HIVE_CMD" -e "USE ${HIVE_DB}; LOAD DATA INPATH '${hdfs_file}' OVERWRITE INTO TABLE ${table};"
}

printf '\n'
print_separator
printf 'SparkMain lightweight Hive pipeline test\n'
print_separator

printf '[CHECK] Checking required commands...\n'
check_command "$HIVE_CMD"
check_command "$HDFS_CMD"
check_command "$PYTHON_CMD"
printf '[SUCCESS] Command check passed\n'

printf '[CHECK] Checking required directories...\n'
check_directory "$DATASET_DIR"
check_directory "$CLEANPY_DIR"
check_directory "$INITIALIZE_SQL_DIR"
check_directory "$PREPARE_DATA_DIR"
check_directory "$JOB_SQL_DIR"
printf '[SUCCESS] Directory check passed\n'

print_step 1 "Prepare cleaned test data"
rm -rf "$CLEANED_DIR"
mkdir -p "$CLEANED_DIR"

mapfile -t py_files < <(find "$CLEANPY_DIR" -name "*.py" | sort)
for py_file in "${py_files[@]}"; do
    printf '[INFO] Running cleaner: %s\n' "$py_file"
    "$PYTHON_CMD" "$py_file"
done

print_step 2 "Stage KB-sized test CSV files in HDFS"
"$HDFS_CMD" dfs -rm -r -f "$HDFS_BASE_PATH" >/dev/null 2>&1 || true
"$HDFS_CMD" dfs -mkdir -p "$HDFS_STAGE_PATH"

stage_csv "${DATASET_DIR}/movies.csv" "${HDFS_STAGE_PATH}/movies.csv"
stage_csv "${DATASET_DIR}/links.csv" "${HDFS_STAGE_PATH}/links.csv"
stage_csv "${DATASET_DIR}/tags.csv" "${HDFS_STAGE_PATH}/tags.csv"

ratings_source="${CLEANED_DIR}/ratings.csv"
if [ ! -f "$ratings_source" ]; then
    ratings_source="${DATASET_DIR}/ratings.csv"
fi
stage_csv "$ratings_source" "${HDFS_STAGE_PATH}/ratings.csv"

print_step 3 "Create Hive database and source tables"
"$HIVE_CMD" -e "CREATE DATABASE IF NOT EXISTS ${HIVE_DB};"
for sql_file in $(find "$INITIALIZE_SQL_DIR" -name "*.sql" | sort); do
    run_hive_file "$sql_file"
done

print_step 4 "Load staged CSV data into Hive tables"
load_table movies "${HDFS_STAGE_PATH}/movies.csv"
load_table links "${HDFS_STAGE_PATH}/links.csv"
load_table tags "${HDFS_STAGE_PATH}/tags.csv"
load_table ratings "${HDFS_STAGE_PATH}/ratings.csv"

print_step 5 "Build prepared Hive objects"
for sql_file in $(find "$PREPARE_DATA_DIR" -name "*.sql" | sort); do
    run_hive_file "$sql_file"
done

print_step 6 "Run Hive analysis SQL"
for sql_file in $(find "$JOB_SQL_DIR" -name "*.sql" | sort); do
    run_hive_file "$sql_file"
done

print_step 7 "Verify results"
bash test/verify_results.sh

printf '\n[SUCCESS] Lightweight Hive pipeline test completed\n'
