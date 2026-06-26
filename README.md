# SparkMain

SparkMain 是一个基于 Apache Spark 3.5 + Scala 的 MovieLens 大数据分析流水线。项目不再依赖 Hive CLI、Hive metastore 或 HDFS staging；批处理由仓库内的 Scala runner 通过 `spark-submit` 执行，结果导出到 `output/`。

## 环境要求

| 组件 | 版本 | 说明 |
| --- | --- | --- |
| JDK | 17 | Maven 编译和 Spark 运行 |
| Apache Spark | 3.5.0 | 批处理与流处理 |
| Scala | 2.12.18 | Spark 3.5 对应 Scala 版本 |
| Maven | 3.8+ | 构建 `SparkMain/pom.xml` |
| Python | 3.8+ | 数据清洗脚本 |
| Kafka | 3.6+ | 实时流处理，可选 |

## 快速开始

```bash
git clone https://github.com/BigData2026QDU/SparkMain.git
cd SparkMain
git submodule update --init --recursive
```

下载 MovieLens 25M 数据集后，将 CSV 放入 `dataset/`，或运行：

```bash
python dataset/download_movielens.py
```

生产流水线：

```bash
chmod +x main_pipeline.sh
./main_pipeline.sh
```

轻量测试流水线：

```bash
chmod +x main_pipeline_test.sh
./main_pipeline_test.sh
```

Windows 可运行：

```cmd
main_pipeline.bat
main_pipeline_test.bat
```

## 批处理流程

| 步骤 | 操作 | 说明 |
| --- | --- | --- |
| 1 | 检查环境 | 需要 `spark-submit`、`mvn`、`python` |
| 2 | 准备数据 | 生产模式会截断大文件，测试模式使用 `dataset_test/` |
| 3 | 清洗数据 | 执行 `cleanPy/` 或 `cleanPy_test/` |
| 4 | 构建模块 | `mvn -B -f SparkMain/pom.xml -DskipTests package` |
| 5 | 执行 Spark SQL | `org.example.pipeline.SparkSqlPipeline` 读取 CSV 并执行 SQL |
| 6 | 导出结果 | `task*` 表导出到 `output/<database>/` |

## SQL 任务规范

在 `jobSQL/` 中创建 SQL 文件，按文件名排序执行，例如 `06_task6_hot_movies.sql`。

SQL 文件应使用当前 Spark database 中的表：

```sql
USE bigdata_ana;

DROP TABLE IF EXISTS task6_hot_movies;

CREATE TABLE task6_hot_movies AS
SELECT ...
FROM ratings
JOIN movies ON ratings.movieId = movies.movieId;

SELECT * FROM task6_hot_movies LIMIT 20;
```

结果表命名使用 `taskN_xxx`，这样流水线会自动导出。

## 可用源表

| 表名 | 字段 |
| --- | --- |
| `movies` | movieId, title, genres |
| `ratings` | userId, movieId, rating, timestamp |
| `tags` | userId, movieId, tag, timestamp |
| `links` | movieId, imdbId, tmdbId |

`prepareData/01_load_data.sql` 会创建 `v_movies_ratings` 视图。

## CI 验证

`.github/workflows/pipeline-validation.yml` 会执行：

1. 检查 `dataset_test/` CSV 总量不超过 64 KiB。
2. 执行 Bash、Python 语法检查。
3. 使用 JDK 17 构建 Scala/Spark 模块。
4. 运行 `ci/run_spark_sql_smoke.py`，在 Spark local 模式下执行 `prepareData_test/` 和 `jobSQL_test/` 并校验结果表。

## 流处理

Kafka + Spark Structured Streaming 模块从 `ratings` topic 读取 JSON，追加写入 Parquet：

```
Kafka Topic (ratings)
    -> Spark Structured Streaming
    -> output/streaming/ratings
```

启动：

```bash
chmod +x start_streaming.sh
./start_streaming.sh
```

生成测试数据：

```bash
cd SparkMain
mvn clean package
java -cp target/spark-streaming-kafka-1.0.0.jar org.example.streaming.RatingProducer
```

## 目录说明

| 路径 | 说明 |
| --- | --- |
| `SparkMain/src/main/scala/org/example/pipeline/` | Spark SQL 批处理 runner |
| `SparkMain/src/main/java/org/example/streaming/` | Kafka 流处理与数据生成器 |
| `dataset/`, `dataset_test/` | 生产和测试 CSV |
| `cleanPy/`, `cleanPy_test/` | 数据清洗脚本 |
| `initializeSQL/`, `initializeSQL_test/` | Spark SQL 源表 schema |
| `prepareData/`, `prepareData_test/` | 数据准备 SQL |
| `jobSQL/`, `jobSQL_test/` | 分析任务 SQL |
| `ci/run_spark_sql_smoke.py` | CI smoke test |
| `output/` | 导出的分析结果 |
