#!/bin/bash
set -euo pipefail

HIVE_DB="bigdata_ana"
HDFS_BASE_PATH="/user/hive/bigdata_ana"
PYTHON_CMD="${PYTHON_CMD:-python3}"

DATASET_DIR="dataset"
TRUNCATED_DIR="truncatedDataset"
CLEANED_DIR="cleanedDataset"
CLEANPY_DIR="cleanPy"
INITIALIZE_SQL_DIR="initializeSQL"
PREPARE_DATA_DIR="prepareData"
JOB_SQL_DIR="jobSQL"

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
    if [ ! -d "$sql_dir" ]; then
        echo "[INFO] SQL directory not found, skipping: $sql_dir"
        return
    fi

    while IFS= read -r sql_file; do
        [ -n "$sql_file" ] || continue
        echo "[INFO] Running SQL: $sql_file"
        hive -f "$sql_file"
    done < <(find "$sql_dir" -name "*.sql" | sort)
}

echo "[INFO] Starting SparkMain production pipeline"

check_command hive
check_command hdfs
check_command "$PYTHON_CMD"
check_directory "$DATASET_DIR"
check_directory "$CLEANPY_DIR"
check_directory "$INITIALIZE_SQL_DIR"
check_directory "$JOB_SQL_DIR"

print_step 1 "Create Hive database if needed"
if hive -e "SHOW DATABASES;" | grep -q "^${HIVE_DB}$"; then
    echo "[INFO] Database exists: $HIVE_DB"
else
    hive -e "CREATE DATABASE ${HIVE_DB};"
    echo "[SUCCESS] Database created: $HIVE_DB"
fi

print_step 2 "Truncate source CSV files"
"$PYTHON_CMD" truncate_file.py --dataset-dir "$DATASET_DIR" --output-dir "$TRUNCATED_DIR" --size 300

print_step 3 "Run Python cleaning scripts"
rm -rf "$CLEANED_DIR"
mkdir -p "$CLEANED_DIR"
while IFS= read -r py_file; do
    [ -n "$py_file" ] || continue
    echo "[INFO] Running cleaner: $py_file"
    "$PYTHON_CMD" "$py_file"
done < <(find "$CLEANPY_DIR" -name "*.py" | sort)

print_step 4 "Upload CSV files to HDFS"
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
done < <(find "$TRUNCATED_DIR" -name "*.csv" | sort)

if [ "$uploaded_count" -eq 0 ]; then
    echo "[ERROR] No CSV files were uploaded"
    exit 1
fi
echo "[SUCCESS] Uploaded CSV files: $uploaded_count"

print_step 5 "Initialize Hive external tables"
run_sql_dir "$INITIALIZE_SQL_DIR"

print_step 6 "Prepare DWD tables and views"
run_sql_dir "$PREPARE_DATA_DIR"

print_step 7 "Run analysis SQL jobs"
run_sql_dir "$JOB_SQL_DIR"

print_step 8 "Pipeline completed"
if [ -f "print_end.sh" ]; then
    bash print_end.sh
else
    echo "END"
fi
