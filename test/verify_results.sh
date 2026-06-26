#!/bin/bash

################################################################################
# 测试验证脚本
# 功能：验证测试流水线执行结果
################################################################################

HIVE_DB="bigdata_ana_test"

echo ""
echo "╔═══════════════════════════════════════════════════════════════════╗"
echo "║                                                                   ║"
echo "║                   测试结果验证                                    ║"
echo "║                                                                   ║"
echo "╚═══════════════════════════════════════════════════════════════════╝"
echo ""

PASS=0
FAIL=0

check_table_exists() {
    local table_name=$1
    if hive -e "USE ${HIVE_DB}; SHOW TABLES;" | grep -q "^${table_name}$"; then
        echo "[✓] 表 '$table_name' 存在"
        PASS=$((PASS + 1))
    else
        echo "[✗] 表 '$table_name' 不存在"
        FAIL=$((FAIL + 1))
    fi
}

check_table_has_data() {
    local table_name=$1
    local count=$(hive -e "USE ${HIVE_DB}; SELECT COUNT(*) FROM ${table_name};" 2>/dev/null | tail -1)
    if [ "$count" -gt 0 ] 2>/dev/null; then
        echo "[✓] 表 '$table_name' 有数据 ($count 行)"
        PASS=$((PASS + 1))
    else
        echo "[✗] 表 '$table_name' 无数据"
        FAIL=$((FAIL + 1))
    fi
}

echo "=========================================="
echo "1. 检查数据库表"
echo "=========================================="

check_table_exists "movies"
check_table_exists "ratings"
check_table_exists "tags"
check_table_exists "links"
check_table_exists "user_behavior"
check_table_exists "dwd_user_behavior_clean"

echo ""
echo "=========================================="
echo "2. 检查数据加载"
echo "=========================================="

check_table_has_data "movies"
check_table_has_data "ratings"
check_table_has_data "tags"
check_table_has_data "links"
check_table_has_data "user_behavior"
check_table_has_data "dwd_user_behavior_clean"

echo ""
echo "=========================================="
echo "3. 检查视图"
echo "=========================================="

if hive -e "USE ${HIVE_DB}; SHOW VIEWS;" | grep -q "v_movies_ratings"; then
    echo "[✓] 视图 'v_movies_ratings' 存在"
    PASS=$((PASS + 1))
else
    echo "[✗] 视图 'v_movies_ratings' 不存在"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "=========================================="
echo "4. 检查分析结果"
echo "=========================================="

check_table_exists "task1_movie_stats"
check_table_has_data "task1_movie_stats"
check_table_exists "lb_funnel_overall"
check_table_has_data "lb_funnel_overall"
check_table_exists "lb_time_hourly_behavior"
check_table_has_data "lb_time_hourly_behavior"
check_table_exists "lb_category_efficiency"
check_table_has_data "lb_category_efficiency"
check_table_exists "lb_user_segment_summary"
check_table_has_data "lb_user_segment_summary"
check_table_exists "lb_user_active_day_distribution"
check_table_has_data "lb_user_active_day_distribution"
check_table_exists "lb_user_retention"
check_table_has_data "lb_user_retention"
check_table_exists "lb_repurchase_behavior_depth"
check_table_has_data "lb_repurchase_behavior_depth"

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
    echo "║                   所有测试通过！                                  ║"
    echo "║                                                                   ║"
    echo "╚═══════════════════════════════════════════════════════════════════╝"
    exit 0
else
    echo "╔═══════════════════════════════════════════════════════════════════╗"
    echo "║                                                                   ║"
    echo "║                   存在失败的测试！                                ║"
    echo "║                                                                   ║"
    echo "╚═══════════════════════════════════════════════════════════════════╝"
    exit 1
fi
