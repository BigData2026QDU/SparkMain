#!/bin/bash

################################################################################
# 大数据分析流水线主脚本（Spark + Scala 版本）
# 功能：自动化处理从数据清洗到分析的全流程
# 用法：
#   ./main_pipeline_new.sh          # 批处理模式
#   ./main_pipeline_new.sh stream   # 实时处理模式
################################################################################

set -e  # 任何命令失败立即退出

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARKMAIN_HOME="${SPARKMAIN_HOME:-$SCRIPT_DIR}"
APP_HOME="$(cd "$SPARKMAIN_HOME" && pwd)"

trim_env_space() {
    local value=$1
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf "%s" "$value"
}

strip_env_quotes() {
    local value=$1
    if [ "${#value}" -ge 2 ]; then
        case "$value" in
            \"*\") value="${value:1:${#value}-2}" ;;
            \'*\') value="${value:1:${#value}-2}" ;;
        esac
    fi
    printf "%s" "$value"
}

is_allowed_env_key() {
    case "$1" in
        PYTHON_CMD|SPARK_MASTER|SPARK_LOCAL_IP|SPARKMAIN_JAR|JAR_PATH|SPARKMAIN_EXTRA_JARS|SPARKMAIN_TEST_MODE)
            return 0
            ;;
        DATASET_DIR|TRUNCATED_DIR|CLEANED_DIR|CLEANPY_DIR|OUTPUT_DIR)
            return 0
            ;;
        KAFKA_TOPIC|KAFKA_BOOTSTRAP_SERVERS|STREAMING_CHECKPOINT_PATH|STREAMING_OUTPUT_PATH)
            return 0
            ;;
        MYSQL_ENABLED|OFFLINE_MYSQL_ENABLED|MYSQL_JDBC_URL|MYSQL_USER|MYSQL_PASSWORD)
            return 0
            ;;
        MYSQL_TABLE|MYSQL_TABLE_RATINGS|MYSQL_TABLE_GENRES|MYSQL_TABLE_TIME|MYSQL_TABLE_USERS)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

warn_ignored_env_line() {
    local env_file=$1
    local line_number=$2
    echo "[警告] 忽略不支持的本地环境配置行: $env_file:$line_number" >&2
}

load_env_file() {
    local env_file=$1
    local line
    local line_number=0
    local key
    local value

    if [ ! -f "$env_file" ]; then
        return 1
    fi

    while IFS= read -r line || [ -n "$line" ]; do
        line_number=$((line_number + 1))
        line="${line%$'\r'}"
        line="$(trim_env_space "$line")"

        case "$line" in
            ""|\#*) continue ;;
        esac

        if [[ "$line" != *=* ]]; then
            warn_ignored_env_line "$env_file" "$line_number"
            continue
        fi

        key="$(trim_env_space "${line%%=*}")"
        value="$(trim_env_space "${line#*=}")"

        if [[ ! "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || ! is_allowed_env_key "$key"; then
            warn_ignored_env_line "$env_file" "$line_number"
            continue
        fi

        value="$(strip_env_quotes "$value")"
        printf -v "$key" "%s" "$value"
        export "$key"
    done < "$env_file"

    echo "[信息] 已加载本地环境配置: $env_file"
    return 0
}

load_local_env() {
    local candidate
    for candidate in \
        "$APP_HOME/.env" \
        "$APP_HOME/sparkmain.env" \
        "$APP_HOME/conf/sparkmain.env" \
        "$SCRIPT_DIR/.env" \
        "$SCRIPT_DIR/sparkmain.env" \
        "$SCRIPT_DIR/conf/sparkmain.env"; do
        if load_env_file "$candidate"; then
            return 0
        fi
    done
    return 0
}

resolve_path() {
    local path_value=$1
    case "$path_value" in
        /*|[A-Za-z]:*) printf "%s\n" "$path_value" ;;
        *) printf "%s\n" "$APP_HOME/$path_value" ;;
    esac
}

discover_default_jar() {
    local jar
    for jar in "$APP_HOME"/lib/sparkmain_2.12-*.jar; do
        if [ -f "$jar" ]; then
            printf "%s\n" "$jar"
            return 0
        fi
    done
    printf "%s\n" "$APP_HOME/SparkMain/target/scala-2.12/sparkmain_2.12-1.0.0.jar"
}

join_distribution_jars() {
    local result=""
    local separator=""
    local jar
    local app_jar_name

    app_jar_name="$(basename "$JAR_PATH")"
    if [ ! -d "$APP_HOME/lib" ]; then
        return 0
    fi

    for jar in "$APP_HOME"/lib/*.jar; do
        if [ ! -f "$jar" ]; then
            continue
        fi
        if [ "$(basename "$jar")" = "$app_jar_name" ]; then
            continue
        fi
        result="${result}${separator}${jar}"
        separator=","
    done

    printf "%s" "$result"
}

spark_submit_main() {
    local extra_jars
    local submit_args

    extra_jars="${SPARKMAIN_EXTRA_JARS:-$(join_distribution_jars)}"
    submit_args=(--class org.bigdata.Main --master "$SPARK_MASTER")
    if [ -n "$extra_jars" ]; then
        submit_args+=(--jars "$extra_jars")
    fi

    spark-submit "${submit_args[@]}" "$JAR_PATH" "$@"
}

load_local_env
cd "$APP_HOME"

################################################################################
# 配置项
################################################################################
PYTHON_CMD="${PYTHON_CMD:-python3}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
CONFIGURED_JAR_PATH="${SPARKMAIN_JAR:-${JAR_PATH:-}}"
if [ -n "$CONFIGURED_JAR_PATH" ]; then
    JAR_PATH="$(resolve_path "$CONFIGURED_JAR_PATH")"
else
    JAR_PATH="$(discover_default_jar)"
fi

# 目录定义
DATASET_DIR="${DATASET_DIR:-dataset}"
TRUNCATED_DIR="${TRUNCATED_DIR:-truncatedDataset}"
CLEANED_DIR="${CLEANED_DIR:-cleanedDataset}"
CLEANPY_DIR="${CLEANPY_DIR:-cleanPy}"
OUTPUT_DIR="${OUTPUT_DIR:-output}"

# 模式
MODE=${1:-"batch"}

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
    if ! command -v "$cmd" &> /dev/null; then
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

env_flag_enabled() {
    local flag_name=$1
    local flag_value="${!flag_name:-false}"
    case "$flag_value" in
        true|TRUE|1|yes|YES) return 0 ;;
        *) return 1 ;;
    esac
}

mysql_export_enabled() {
    env_flag_enabled "MYSQL_ENABLED"
}

offline_mysql_export_enabled() {
    env_flag_enabled "OFFLINE_MYSQL_ENABLED"
}

run_spark_task() {
    local task=$1
    local description=$2
    local output_path="$OUTPUT_DIR/$task"

    echo "[信息] 运行${description}..."
    spark_submit_main \
        "$task" \
        "$CLEANED_DIR" \
        "$output_path"

    echo "[成功] ${description}完成，输出目录: $output_path"
}

################################################################################
# 实时处理模式
################################################################################

if [ "$MODE" = "stream" ]; then
    echo ""
    echo "╔═══════════════════════════════════════════════════════════════════╗"
    echo "║                                                                   ║"
    echo "║                   实时数据处理工作流（Kafka + Spark → MySQL）     ║"
    echo "║                                                                   ║"
    echo "╚═══════════════════════════════════════════════════════════════════╝"
    echo ""

    # 环境检查
    echo "[检查] 检查必要的命令..."
    check_command spark-submit
    echo "[成功] 所有命令检查通过"

    # 检查 JAR 文件
    if [ ! -f "$JAR_PATH" ]; then
        echo "[错误] JAR 文件不存在: $JAR_PATH"
        echo "[提示] 请先运行 'sbt package' 构建项目"
        exit 1
    fi

    echo "[信息] 启动实时数据处理工作流..."
    if mysql_export_enabled; then
        echo "[信息] MySQL 导出: 已启用（连接信息来自环境变量，日志不输出 JDBC URL）"
    else
        echo "[信息] MySQL 导出: 未启用"
    fi

    spark_submit_main realtime

    exit 0
fi

################################################################################
# 批处理模式（默认）
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
check_command "$PYTHON_CMD"
echo "[成功] 所有命令检查通过"

echo "[检查] 检查必要的目录..."
check_directory "$DATASET_DIR"
check_directory "$CLEANPY_DIR"
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
"$PYTHON_CMD" truncate_file.py
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
rm -rf "$CLEANED_DIR"/*
mkdir -p "$CLEANED_DIR"

py_files=$(find "$CLEANPY_DIR" -name "*.py" | sort)

if [ -z "$py_files" ]; then
    echo "[警告] '$CLEANPY_DIR' 目录下没有找到 Python 脚本"
else
    for py_file in $py_files; do
        echo "[信息] 正在执行: $py_file"
        "$PYTHON_CMD" "$py_file"
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
rm -rf "$OUTPUT_DIR"/*
mkdir -p "$OUTPUT_DIR"

if offline_mysql_export_enabled; then
    echo "[信息] 离线 MySQL 导出: 已启用（连接信息来自环境变量，日志不输出 JDBC URL）"
else
    echo "[信息] 离线 MySQL 导出: 未启用"
fi

run_spark_task "ratings" "评分质量分析"
run_spark_task "genres" "类型热度分析"
run_spark_task "time" "时间趋势分析"
run_spark_task "users" "用户行为分析"

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
