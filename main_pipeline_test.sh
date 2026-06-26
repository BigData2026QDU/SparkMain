#!/bin/bash
set -euo pipefail

HIVE_DB="bigdata_ana_test"
HDFS_BASE_PATH="/user/hive/bigdata_ana_test"
PYTHON_CMD="${PYTHON_CMD:-python3}"
HIVE_EXECUTION_ENGINE="${HIVE_EXECUTION_ENGINE:-}"

DATASET_DIR="dataset_test"
CLEANED_DIR="cleanedDataset_test"
CLEANPY_DIR="cleanPy_test"
INITIALIZE_SQL_DIR="initializeSQL_test"
PREPARE_DATA_DIR="prepareData_test"
JOB_SQL_DIR="jobSQL_test"

print_step() {
    echo ""
    echo "================================================================================"
    echo "Step $1: $2"
    echo "================================================================================"
}

check_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "[ERROR] Command not found: $1"
        exit 1
    fi
}

check_directory() {
    if [ ! -d "$1" ]; then
        echo "[ERROR] Directory not found: $1"
        exit 1
    fi
}

upload_csv() {
    local csv_file="$1"
    local filename
    local table_name
    filename="$(basename "$csv_file")"
    table_name="${filename%.csv}"

    echo "[INFO] Uploading $filename to ${HDFS_BASE_PATH}/${table_name}/"
    hdfs dfs -mkdir -p "${HDFS_BASE_PATH}/${table_name}"
    hdfs dfs -put -f "$csv_file" "${HDFS_BASE_PATH}/${table_name}/"
    hdfs dfs -test -e "${HDFS_BASE_PATH}/${table_name}/${filename}"
}

run_sql_dir() {
    local sql_dir="$1"
    local temp_sql
    if [ ! -d "$sql_dir" ]; then
        echo "[INFO] SQL directory not found, skipping: $sql_dir"
        return
    fi

    while IFS= read -r sql_file; do
        [ -n "$sql_file" ] || continue
        echo "[INFO] Running SQL: $sql_file"
        if [ -n "$HIVE_EXECUTION_ENGINE" ]; then
            temp_sql="$(mktemp)"
            sed "s/SET hive.execution.engine=[^;]*;/SET hive.execution.engine=${HIVE_EXECUTION_ENGINE};/g" "$sql_file" > "$temp_sql"
            hive -f "$temp_sql"
            rm -f "$temp_sql"
        else
            hive -f "$sql_file"
        fi
    done < <(find "$sql_dir" -name "*.sql" | sort)
}

echo "[INFO] Starting SparkMain test pipeline"

check_command hive
check_command hdfs
check_command "$PYTHON_CMD"
check_directory "$DATASET_DIR"
check_directory "$CLEANPY_DIR"
check_directory "$INITIALIZE_SQL_DIR"
check_directory "$JOB_SQL_DIR"

print_step 1 "Create Hive test database if needed"
if hive -e "SHOW DATABASES;" | grep -q "^${HIVE_DB}$"; then
    echo "[INFO] Database exists: $HIVE_DB"
else
    hive -e "CREATE DATABASE ${HIVE_DB};"
    echo "[SUCCESS] Database created: $HIVE_DB"
fi

print_step 2 "Skip truncation for lightweight test data"
echo "[INFO] Test data is already small"

print_step 3 "Run Python test cleaning scripts"
rm -rf "$CLEANED_DIR"
mkdir -p "$CLEANED_DIR"
while IFS= read -r py_file; do
    [ -n "$py_file" ] || continue
    echo "[INFO] Running cleaner: $py_file"
    "$PYTHON_CMD" "$py_file"
done < <(find "$CLEANPY_DIR" -name "*.py" | sort)

print_step 4 "Upload test CSV files to HDFS"
hdfs dfs -rm -r -f "${HDFS_BASE_PATH:?}/"* 2>/dev/null || true
hdfs dfs -mkdir -p "$HDFS_BASE_PATH"

uploaded_count=0
while IFS= read -r csv_file; do
    [ -n "$csv_file" ] || continue
    upload_csv "$csv_file"
    uploaded_count=$((uploaded_count + 1))
done < <(find "$CLEANED_DIR" -name "*.csv" | sort)

while IFS= read -r csv_file; do
    [ -n "$csv_file" ] || continue
    filename="$(basename "$csv_file")"
    if [ "$filename" = "UserBehavior.csv" ] && [ -f "$CLEANED_DIR/user_behavior.csv" ]; then
        continue
    fi
    if [ -f "$CLEANED_DIR/$filename" ]; then
        continue
    fi
    upload_csv "$csv_file"
    uploaded_count=$((uploaded_count + 1))
done < <(find "$DATASET_DIR" -name "*.csv" | sort)

if [ "$uploaded_count" -eq 0 ]; then
    echo "[ERROR] No CSV files were uploaded"
    exit 1
fi
echo "[SUCCESS] Uploaded CSV files: $uploaded_count"

print_step 5 "Initialize Hive test tables"
run_sql_dir "$INITIALIZE_SQL_DIR"

print_step 6 "Prepare test DWD tables and views"
run_sql_dir "$PREPARE_DATA_DIR"

print_step 7 "Run test analysis SQL jobs"
run_sql_dir "$JOB_SQL_DIR"

print_step 8 "Test pipeline completed"
echo "[COMPLETED] Test pipeline execution completed successfully"
