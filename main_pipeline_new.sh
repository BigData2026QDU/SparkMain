#!/bin/bash

################################################################################
# 大数据分析流水线主脚本（Spark + Scala 版本）
# 功能：自动化处理数据清洗到分析的全流程
################################################################################

set -e  # 任何命令失败立即退出

################################################################################
# 配置项
################################################################################
PYTHON_CMD="python3"
SPARK_MASTER="local[*]"
JAR_PATH="SparkMain/target/scala-2.12/sparkmain_2.12-1.0.jar"

# 目录定义
DATASET_DIR="dataset"
TRUNCATED_DIR="truncatedDataset"
CLEANED_DIR="cleanedDataset"
CLEANPY_DIR="cleanPy"
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
echo "║                   大数据分析流水线（Spark + Scala）               ║"
echo "║                                                                   ║"
echo "╚═══════════════════════════════════════════════════════════════════╝"
echo ""

# 环境检查
echo "[检查] 检查必要的命令..."
check_command spark-submit
check_command $PYTHON_CMD
echo "[成功] 所有命令检查通过"

echo "[检查] 检查必要的目录..."
check_directory $DATASET_DIR
check_directory $CLEANPY_DIR
echo "[成功] 所有目录检查通过"

# 检查 JAR 文件
if [ ! -f "$JAR_PATH" ]; then
    echo "[错误] JAR 文件不存在: $JAR_PATH"
    echo "[提示] 请先运行 'sbt package' 构建项目"
    exit 1
fi

################################################################################
# 步骤1: 运行 truncate_file.py
################################################################################
print_step 1 "运行 truncate_file.py 处理原始数据"

echo "[信息] 正在执行 truncate_file.py..."
$PYTHON_CMD truncate_file.py
if [ $? -eq 0 ]; then
    echo "[成功] truncate_file.py 执行成功"
else
    echo "[错误] truncate_file.py 执行失败"
    exit 1
fi

################################################################################
# 步骤2: 运行 cleanPy 目录下的所有 Python 脚本
################################################################################
print_step 2 "运行 cleanPy 目录下的所有清洗脚本"

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
# 步骤3: 运行 Spark 分析任务
################################################################################
print_step 3 "运行 Spark 分析任务"

echo "[信息] 清空 '$OUTPUT_DIR' 目录..."
rm -rf $OUTPUT_DIR/*
mkdir -p $OUTPUT_DIR

echo "[信息] 运行评分分析..."
spark-submit \
    --class org.example.analysis.AnalyzeRatings \
    --master $SPARK_MASTER \
    $JAR_PATH

if [ $? -ne 0 ]; then
    echo "[错误] 评分分析失败"
    exit 1
fi
echo "[成功] 评分分析完成"

echo "[信息] 运行类型分析..."
spark-submit \
    --class org.example.analysis.AnalyzeGenres \
    --master $SPARK_MASTER \
    $JAR_PATH

if [ $? -ne 0 ]; then
    echo "[错误] 类型分析失败"
    exit 1
fi
echo "[成功] 类型分析完成"

echo "[信息] 运行时间分析..."
spark-submit \
    --class org.example.analysis.AnalyzeTime \
    --master $SPARK_MASTER \
    $JAR_PATH

if [ $? -ne 0 ]; then
    echo "[错误] 时间分析失败"
    exit 1
fi
echo "[成功] 时间分析完成"

echo "[信息] 运行用户行为分析..."
spark-submit \
    --class org.example.analysis.AnalyzeUsers \
    --master $SPARK_MASTER \
    $JAR_PATH

if [ $? -ne 0 ]; then
    echo "[错误] 用户行为分析失败"
    exit 1
fi
echo "[成功] 用户行为分析完成"

################################################################################
# 步骤4: 打印结束标记
################################################################################
print_step 4 "流水线执行完成"

echo ""
echo "分析结果已保存到: $OUTPUT_DIR/"
echo ""

if [ -f "print_end.sh" ]; then
    bash print_end.sh
else
    echo "[警告] print_end.sh 文件不存在"
    echo ""
    echo "END"
    echo ""
fi

exit 0
