#!/bin/bash

################################################################################
# 大数据分析流水线主脚本
# 功能：自动化处理从数据清洗到Hive分析的全流程
################################################################################

set -e  # 任何命令失败立即退出

################################################################################
# 配置项
################################################################################
HIVE_DB="bigdata_ana"
HDFS_BASE_PATH="/user/hive/bigdata_ana"
PYTHON_CMD="python3"

# 目录定义
DATASET_DIR="dataset"
TRUNCATED_DIR="truncatedDataset"
CLEANED_DIR="cleanedDataset"
CLEANPY_DIR="cleanPy"
INITIALIZE_SQL_DIR="initializeSQL"
PREPARE_DATA_DIR="prepareData"
JOB_SQL_DIR="jobSQL"

################################################################################
# 工具函数
################################################################################

# 打印分隔线
print_separator() {
    echo "================================================================================"
}

# 打印步骤标题
print_step() {
    local step_num=$1
    local step_desc=$2
    echo ""
    print_separator
    echo "步骤 $step_num: $step_desc"
    print_separator
}

# 检查命令是否存在
check_command() {
    local cmd=$1
    if ! command -v $cmd &> /dev/null; then
        echo "[错误] 命令 '$cmd' 未找到，请先安装并配置环境变量"
        exit 1
    fi
}

# 检查目录是否存在
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
echo "║                   大数据分析流水线启动                            ║"
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

# 检查数据库是否存在
if hive -e "SHOW DATABASES;" | grep -q "^${HIVE_DB}$"; then
    echo "[信息] 数据库 '$HIVE_DB' 已存在"
else
    echo "[信息] 数据库 '$HIVE_DB' 不存在，正在创建..."
    hive -e "CREATE DATABASE ${HIVE_DB};"
    echo "[成功] 数据库 '$HIVE_DB' 创建成功"
fi

################################################################################
# 步骤2: 运行 truncate_file.py
################################################################################
print_step 2 "运行 truncate_file.py 处理原始数据"

echo "[信息] 正在执行 truncate_file.py..."
$PYTHON_CMD truncate_file.py
if [ $? -eq 0 ]; then
    echo "[成功] truncate_file.py 执行成功"
else
    echo "[错误] truncate_file.py 执行失败"
    exit 1
fi

################################################################################
# 步骤3: 运行 cleanPy 目录下的所有 Python 脚本
################################################################################
print_step 3 "运行 cleanPy 目录下的所有清洗脚本"

# 清空 cleanedDataset 目录
echo "[信息] 清空 '$CLEANED_DIR' 目录..."
rm -rf $CLEANED_DIR/*
mkdir -p $CLEANED_DIR

# 获取所有 .py 文件并按字母顺序执行
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

# 清理HDFS旧文件
echo "[信息] 清理 HDFS 路径: $HDFS_BASE_PATH"
hdfs dfs -rm -r -f $HDFS_BASE_PATH/* 2>/dev/null || true

# 确保HDFS目录存在
hdfs dfs -mkdir -p $HDFS_BASE_PATH

# 上传 cleanedDataset 中的所有文件
csv_files=$(find $CLEANED_DIR -name "*.csv")
if [ -z "$csv_files" ]; then
    echo "[警告] '$CLEANED_DIR' 目录下没有找到 CSV 文件"
else
    for csv_file in $csv_files; do
        filename=$(basename $csv_file)
        echo "[信息] 正在上传: $filename"
        hdfs dfs -put $csv_file $HDFS_BASE_PATH/

        # 验证上传成功
        if hdfs dfs -test -e $HDFS_BASE_PATH/$filename; then
            echo "[成功] $filename 上传成功"
        else
            echo "[错误] $filename 上传失败"
            exit 1
        fi
    done
fi

echo "[信息] 验证HDFS文件数量..."
hdfs_count=$(hdfs dfs -ls $HDFS_BASE_PATH/*.csv 2>/dev/null | wc -l)
local_count=$(ls -1 $CLEANED_DIR/*.csv 2>/dev/null | wc -l)
echo "[信息] 本地CSV文件数: $local_count, HDFS文件数: $hdfs_count"

if [ $hdfs_count -eq $local_count ]; then
    echo "[成功] 所有文件已成功上传到 HDFS"
else
    echo "[错误] HDFS文件数量与本地不一致"
    exit 1
fi

################################################################################
# 步骤5: 检查Hive表是否存在，决定是否初始化
################################################################################
print_step 5 "检查 Hive 表并决定是否执行初始化"

need_initialize=false

# 获取所有CSV文件对应的表名
for csv_file in $CLEANED_DIR/*.csv; do
    if [ -f "$csv_file" ]; then
        filename=$(basename $csv_file)
        table_name="${filename%.csv}"  # 去掉.csv后缀

        echo "[检查] 检查表: $table_name"

        # 检查表是否存在
        if hive -e "USE ${HIVE_DB}; SHOW TABLES;" | grep -q "^${table_name}$"; then
            echo "[信息] 表 '$table_name' 已存在"
        else
            echo "[信息] 表 '$table_name' 不存在"
            need_initialize=true
        fi
    fi
done

# 如果有任意表不存在，执行所有初始化SQL
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
# 步骤6: 运行 prepareData 目录下的所有SQL
################################################################################
print_step 6 "运行 prepareData 目录下的所有 SQL 脚本"

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
# 步骤7: 运行 jobSQL 目录下的所有SQL
################################################################################
print_step 7 "运行 jobSQL 目录下的所有分析任务"

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
print_step 8 "流水线执行完成"

if [ -f "print_end.sh" ]; then
    bash print_end.sh
else
    echo "[警告] print_end.sh 文件不存在"
    echo ""
    echo "END"
    echo ""
fi

exit 0