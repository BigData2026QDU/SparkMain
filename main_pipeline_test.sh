#!/bin/bash

################################################################################
# 大数据分析流水线测试脚本
# 功能：使用轻量级测试数据运行流水线
################################################################################

set -e  # 任何命令失败立即退出

################################################################################
# 配置项
################################################################################
HIVE_DB="bigdata_ana_test"
HDFS_BASE_PATH="/user/hive/bigdata_ana_test"
PYTHON_CMD="python3"

# 目录定义（测试版本）
DATASET_DIR="dataset_test"
TRUNCATED_DIR="truncatedDataset_test"
CLEANED_DIR="cleanedDataset_test"
CLEANPY_DIR="cleanPy_test"
INITIALIZE_SQL_DIR="initializeSQL_test"
PREPARE_DATA_DIR="prepareData_test"
JOB_SQL_DIR="jobSQL_test"

################################################################################
# 工具函数
################################################################################

print_separator() {
    echo "================================================================================"
}

print_step() {
    local step_num=$1
    local step_desc=$2
    echo ""
    print_separator
    echo "步骤 $step_num: $step_desc"
    print_separator
}

check_command() {
    local cmd=$1
    if ! command -v $cmd &> /dev/null; then
        echo "[错误] 命令 '$cmd' 未找到，请先安装并配置环境变量"
        exit 1
    fi
}

check_directory() {
    local dir=$1
    if [ ! -d "$dir" ]; then
        echo "[错误] 目录 '$dir' 不存在"
        exit 1
    fi
}

################################################################################
# 主流程
################################################################################

echo ""
echo "╔═══════════════════════════════════════════════════════════════════╗"
echo "║                                                                   ║"
echo "║                   大数据分析流水线（测试模式）                    ║"
echo "║                                                                   ║"
echo "╚═══════════════════════════════════════════════════════════════════╝"
echo ""

# 环境检查
echo "[检查] 检查必要的命令..."
check_command hive
check_command hdfs
check_command $PYTHON_CMD
echo "[成功] 所有命令检查通过"

echo "[检查] 检查必要的目录..."
check_directory $DATASET_DIR
check_directory $CLEANPY_DIR
check_directory $INITIALIZE_SQL_DIR
check_directory $JOB_SQL_DIR
echo "[成功] 所有目录检查通过"

################################################################################
# 步骤1: 检查/创建Hive数据库
################################################################################
print_step 1 "检查/创建 Hive 数据库 '$HIVE_DB'"

if hive -e "SHOW DATABASES;" | grep -q "^${HIVE_DB}$"; then
    echo "[信息] 数据库 '$HIVE_DB' 已存在"
else
    echo "[信息] 数据库 '$HIVE_DB' 不存在，正在创建..."
    hive -e "CREATE DATABASE ${HIVE_DB};"
    echo "[成功] 数据库 '$HIVE_DB' 创建成功"
fi

################################################################################
# 步骤2: 运行 truncate_file.py（测试模式跳过）
################################################################################
print_step 2 "跳过 truncate_file.py（测试数据已足够小）"

################################################################################
# 步骤3: 运行 cleanPy_test 目录下的所有 Python 脚本
################################################################################
print_step 3 "运行 cleanPy_test 目录下的所有清洗脚本"

echo "[信息] 清空 '$CLEANED_DIR' 目录..."
rm -rf $CLEANED_DIR/*
mkdir -p $CLEANED_DIR

py_files=$(find $CLEANPY_DIR -name "*.py" | sort)

if [ -z "$py_files" ]; then
    echo "[警告] '$CLEANPY_DIR' 目录下没有找到 Python 脚本"
else
    for py_file in $py_files; do
        echo "[信息] 正在执行: $py_file"
        $PYTHON_CMD $py_file
        if [ $? -ne 0 ]; then
            echo "[错误] $py_file 执行失败"
            exit 1
        fi
        echo "[成功] $py_file 执行成功"
    done
fi

################################################################################
# 步骤4: 清理HDFS旧文件并上传新文件
################################################################################
print_step 4 "清理 HDFS 并上传清洗后的数据"

echo "[信息] 清理 HDFS 路径: $HDFS_BASE_PATH"
hdfs dfs -rm -r -f $HDFS_BASE_PATH/* 2>/dev/null || true

hdfs dfs -mkdir -p $HDFS_BASE_PATH

# 上传原始测试数据（因为测试数据已经很小）
for csv_file in $DATASET_DIR/*.csv; do
    if [ -f "$csv_file" ]; then
        filename=$(basename $csv_file)
        echo "[信息] 正在上传: $filename"
        hdfs dfs -put $csv_file $HDFS_BASE_PATH/
        
        if hdfs dfs -test -e $HDFS_BASE_PATH/$filename; then
            echo "[成功] $filename 上传成功"
        else
            echo "[错误] $filename 上传失败"
            exit 1
        fi
    fi
done

# 如果有清洗后的数据，也上传
csv_files=$(find $CLEANED_DIR -name "*.csv")
if [ -n "$csv_files" ]; then
    for csv_file in $csv_files; do
        filename=$(basename $csv_file)
        echo "[信息] 正在上传清洗后的: $filename"
        hdfs dfs -put $csv_file $HDFS_BASE_PATH/
    done
fi

echo "[信息] 验证HDFS文件数量..."
hdfs_count=$(hdfs dfs -ls $HDFS_BASE_PATH/*.csv 2>/dev/null | wc -l)
echo "[信息] HDFS文件数: $hdfs_count"

################################################################################
# 步骤5: 检查Hive表是否存在，决定是否初始化
################################################################################
print_step 5 "检查 Hive 表并决定是否执行初始化"

need_initialize=false

for csv_file in $DATASET_DIR/*.csv; do
    if [ -f "$csv_file" ]; then
        filename=$(basename $csv_file)
        table_name="${filename%.csv}"

        echo "[检查] 检查表: $table_name"

        if hive -e "USE ${HIVE_DB}; SHOW TABLES;" | grep -q "^${table_name}$"; then
            echo "[信息] 表 '$table_name' 已存在"
        else
            echo "[信息] 表 '$table_name' 不存在"
            need_initialize=true
        fi
    fi
done

if [ "$need_initialize" = true ]; then
    echo "[信息] 检测到缺失的表，开始执行所有初始化SQL..."

    sql_files=$(find $INITIALIZE_SQL_DIR -name "*.sql" | sort)

    if [ -z "$sql_files" ]; then
        echo "[警告] '$INITIALIZE_SQL_DIR' 目录下没有找到 SQL 文件"
    else
        for sql_file in $sql_files; do
            echo "[信息] 正在执行: $sql_file"
            hive -f $sql_file
            if [ $? -ne 0 ]; then
                echo "[错误] $sql_file 执行失败"
                exit 1
            fi
            echo "[成功] $sql_file 执行成功"
        done
    fi
else
    echo "[信息] 所有表都已存在，跳过初始化"
fi

################################################################################
# 步骤6: 运行 prepareData_test 目录下的所有SQL
################################################################################
print_step 6 "运行 prepareData_test 目录下的所有 SQL 脚本"

if [ -d "$PREPARE_DATA_DIR" ]; then
    sql_files=$(find $PREPARE_DATA_DIR -name "*.sql" | sort)

    if [ -z "$sql_files" ]; then
        echo "[信息] '$PREPARE_DATA_DIR' 目录下没有找到 SQL 文件，跳过此步骤"
    else
        for sql_file in $sql_files; do
            echo "[信息] 正在执行: $sql_file"
            hive -f $sql_file
            if [ $? -ne 0 ]; then
                echo "[错误] $sql_file 执行失败"
                exit 1
            fi
            echo "[成功] $sql_file 执行成功"
        done
    fi
else
    echo "[信息] '$PREPARE_DATA_DIR' 目录不存在，跳过此步骤"
fi

################################################################################
# 步骤7: 运行 jobSQL_test 目录下的所有SQL
################################################################################
print_step 7 "运行 jobSQL_test 目录下的所有分析任务"

sql_files=$(find $JOB_SQL_DIR -name "*.sql" | sort)

if [ -z "$sql_files" ]; then
    echo "[警告] '$JOB_SQL_DIR' 目录下没有找到 SQL 文件"
else
    for sql_file in $sql_files; do
        echo "[信息] 正在执行: $sql_file"
        hive -f $sql_file
        if [ $? -ne 0 ]; then
            echo "[错误] $sql_file 执行失败"
            exit 1
        fi
        echo "[成功] $sql_file 执行成功"
    done
fi

################################################################################
# 步骤8: 打印结束标记
################################################################################
print_step 8 "测试流水线执行完成"

echo ""
echo "╔═══════════════════════════════════════════════════════════════════╗"
echo "║                                                                   ║"
echo "║                   测试流水线执行成功！                            ║"
echo "║                                                                   ║"
echo "╚═══════════════════════════════════════════════════════════════════╝"
echo ""

exit 0
