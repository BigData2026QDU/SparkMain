# 系统架构

## 1. 整体架构

SparkMain 是一个基于 Apache Spark 3.5 和 Scala 的大数据处理项目，负责 MovieLens 数据清洗、Spark SQL 分析、结果导出和 Kafka 增量流处理。

## 2. 核心模块

### 2.1 数据清洗模块

- 使用 Python 脚本清洗原始 CSV。
- 生产模式输出到 `cleanedDataset/`。
- 测试模式输出到 `cleanedDataset_test/`。

### 2.2 Spark SQL 批处理模块

- `org.example.pipeline.SparkSqlPipeline` 通过 `spark-submit` 运行。
- 直接读取本地或分布式文件系统中的 CSV。
- 在 Spark catalog 中创建源表，执行 `prepareData/` 和 `jobSQL/` 下的 SQL。
- 将 `task*` 结果表导出到 `output/<database>/`。

### 2.3 Spark Structured Streaming 模块

- 从 Kafka topic `ratings` 消费 JSON 评分事件。
- 使用 Spark Structured Streaming 解析并追加写入 Parquet 数据集。
- checkpoint 默认位于 `/tmp/spark/checkpoints/ratings`。

## 3. 技术选型

- JDK 17
- Scala 2.12.18
- Apache Spark 3.5.0
- Maven
- Python 3.8+
- Kafka 3.6+

## 4. 数据流

```
原始 CSV -> Python 清洗 -> Spark SQL 源表 -> SQL 分析任务 -> CSV 结果导出
Kafka ratings topic -> Spark Structured Streaming -> Parquet 增量结果
```

## 5. 部署方式

- 本地开发：`main_pipeline_test.sh` 或 `main_pipeline_test.bat`
- 生产数据：`main_pipeline.sh` 或 `main_pipeline.bat`
- CI：GitHub Actions 使用 Spark local 模式运行轻量 smoke test
