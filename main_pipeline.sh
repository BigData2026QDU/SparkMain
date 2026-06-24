#!/bin/bash

################################################################################
# 大数据分析流水线主脚本
# 功能：自动化处理从数据清洗到Spark分析的全流程
################################################################################

set -e  # 任何命令失败立即退出

################################################################################
# 配置项
################################################################################
PYTHON_CMD="python3"
SPARK_CMD="spark-submit"

# 目录定义
DATASET_DIR="dataset"
TRUNCATED_DIR="truncatedDataset"
CLEANED_DIR="cleanedDataset"
CLEANPY_DIR="cleanPy"
JOB_SQL_DIR="jobSQL"
OUTPUT_DIR="output"

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
echo "║                   大数据分析流水线启动 (SparkSQL)                 ║"
echo "║                                                                   ║"
echo "╚═══════════════════════════════════════════════════════════════════╝"
echo ""

# 环境检查
echo "[检查] 检查必要的命令..."
check_command $PYTHON_CMD
check_command spark-submit
echo "[成功] 所有命令检查通过"

echo "[检查] 检查必要的目录..."
check_directory $DATASET_DIR
check_directory $JOB_SQL_DIR
echo "[成功] 所有目录检查通过"

################################################################################
# 步骤1: 下载数据集（如果不存在）
################################################################################
print_step 1 "检查数据集"

if [ -f "$DATASET_DIR/ratings.csv" ]; then
    echo "[信息] 数据集已存在，跳过下载"
else
    echo "[信息] 数据集不存在，正在下载..."
    $PYTHON_CMD $DATASET_DIR/download_movielens.py
    if [ $? -ne 0 ]; then
        echo "[错误] 数据集下载失败"
        exit 1
    fi
fi

################################################################################
# 步骤2: 运行数据清洗脚本
################################################################################
print_step 2 "运行数据清洗脚本"

# 清空输出目录
echo "[信息] 清空 '$OUTPUT_DIR' 目录..."
rm -rf $OUTPUT_DIR/*
mkdir -p $OUTPUT_DIR

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
# 步骤3: 运行 Spark 分析任务
################################################################################
print_step 3 "运行 Spark 分析任务"

py_files=$(find $JOB_SQL_DIR -name "*.py" | sort)

if [ -z "$py_files" ]; then
    echo "[警告] '$JOB_SQL_DIR' 目录下没有找到 Python 脚本"
else
    for py_file in $py_files; do
        echo "[信息] 正在执行: $py_file"
        spark-submit --master local[*] $py_file
        if [ $? -ne 0 ]; then
            echo "[错误] $py_file 执行失败"
            exit 1
        fi
        echo "[成功] $py_file 执行成功"
    done
fi

################################################################################
# 步骤4: 验证输出
################################################################################
print_step 4 "验证输出结果"

if [ -d "$OUTPUT_DIR" ]; then
    echo "[信息] 输出目录内容:"
    ls -la $OUTPUT_DIR/
else
    echo "[警告] 输出目录不存在"
fi

################################################################################
# 步骤5: 打印结束标记
################################################################################
print_step 5 "流水线执行完成"

if [ -f "print_end.sh" ]; then
    bash print_end.sh
else
    echo "[警告] print_end.sh 文件不存在"
    echo ""
    echo "END"
    echo ""
fi

exit 0
