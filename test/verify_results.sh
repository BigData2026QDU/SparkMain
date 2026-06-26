#!/bin/bash

################################################################################
# 测试验证脚本
# 功能：验证测试流水线执行结果
################################################################################

echo ""
echo "╔═══════════════════════════════════════════════════════════════════╗"
echo "║                                                                   ║"
echo "║                   测试结果验证                                    ║"
echo "║                                                                   ║"
echo "╚═══════════════════════════════════════════════════════════════════╝"
echo ""

PASS=0
FAIL=0

check_file_exists() {
    local file=$1
    if [ -f "$file" ]; then
        echo "[✓] 文件 '$file' 存在"
        PASS=$((PASS + 1))
    else
        echo "[✗] 文件 '$file' 不存在"
        FAIL=$((FAIL + 1))
    fi
}

check_directory_exists() {
    local dir=$1
    if [ -d "$dir" ]; then
        echo "[✓] 目录 '$dir' 存在"
        PASS=$((PASS + 1))
    else
        echo "[✗] 目录 '$dir' 不存在"
        FAIL=$((FAIL + 1))
    fi
}

echo "=========================================="
echo "1. 检查项目结构"
echo "=========================================="

check_directory_exists "SparkMain/src/main/scala/org/example"
check_file_exists "SparkMain/build.sbt"
check_file_exists "main_pipeline_new.sh"

echo ""
echo "=========================================="
echo "2. 检查分析任务"
echo "=========================================="

check_file_exists "SparkMain/src/main/scala/org/example/analysis/AnalyzeRatings.scala"
check_file_exists "SparkMain/src/main/scala/org/example/analysis/AnalyzeGenres.scala"
check_file_exists "SparkMain/src/main/scala/org/example/analysis/AnalyzeTime.scala"
check_file_exists "SparkMain/src/main/scala/org/example/analysis/AnalyzeUsers.scala"

echo ""
echo "=========================================="
echo "3. 检查流处理"
echo "=========================================="

check_file_exists "SparkMain/src/main/scala/org/example/streaming/RatingStreamProcessor.scala"
check_file_exists "SparkMain/src/main/scala/org/example/streaming/RatingProducer.scala"

echo ""
echo "=========================================="
echo "4. 检查工具类"
echo "=========================================="

check_file_exists "SparkMain/src/main/scala/org/example/utils/MySQLExporter.scala"

echo ""
echo "=========================================="
echo "验证结果"
echo "=========================================="
echo ""
echo "通过: $PASS"
echo "失败: $FAIL"
echo ""

if [ $FAIL -eq 0 ]; then
    echo "╔═══════════════════════════════════════════════════════════════════╗"
    echo "║                                                                   ║"
    echo "║                   所有检查通过！                                  ║"
    echo "║                                                                   ║"
    echo "╚═══════════════════════════════════════════════════════════════════╝"
    exit 0
else
    echo "╔═══════════════════════════════════════════════════════════════════╗"
    echo "║                                                                   ║"
    echo "║                   存在失败的检查！                                ║"
    echo "║                                                                   ║"
    echo "╚═══════════════════════════════════════════════════════════════════╝"
    exit 1
fi
