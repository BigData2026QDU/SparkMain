# 文件索引

## 根目录

| 文件路径 | 作用 | 说明 |
| --- | --- | --- |
| `README.md` | 项目说明 | Spark + Scala 批处理和流处理入口文档 |
| `Architecture.md` | 架构说明 | 当前 Spark SQL 架构 |
| `File_Index.md` | 文件索引 | 仓库文件说明 |
| `main_pipeline.sh` | Linux/macOS 生产流水线 | 清洗、构建、运行 Spark SQL |
| `main_pipeline.bat` | Windows 生产流水线 | 清洗、构建、运行 Spark SQL |
| `main_pipeline_test.sh` | Linux/macOS 测试流水线 | 使用 `dataset_test/` 运行轻量验证 |
| `main_pipeline_test.bat` | Windows 测试流水线 | 使用 `dataset_test/` 运行轻量验证 |
| `start_streaming.sh` | 流处理启动脚本 | 启动 Kafka topic 和 Spark Structured Streaming |
| `.github/workflows/pipeline-validation.yml` | CI 工作流 | 构建并运行 Spark SQL smoke test |

## 代码目录

| 文件路径 | 作用 | 说明 |
| --- | --- | --- |
| `SparkMain/pom.xml` | Maven 配置 | Spark、Scala、Kafka 依赖与打包 |
| `SparkMain/src/main/scala/org/example/pipeline/SparkSqlPipeline.scala` | 批处理 runner | 读取 CSV、执行 SQL、校验并导出结果 |
| `SparkMain/src/main/java/org/example/streaming/RatingStreamProcessor.java` | 流处理程序 | 从 Kafka 读取评分并写入 Parquet |
| `SparkMain/src/main/java/org/example/streaming/RatingProducer.java` | 测试生产者 | 向 Kafka 写入模拟评分事件 |

## 数据与 SQL

| 文件路径 | 作用 | 说明 |
| --- | --- | --- |
| `dataset/`, `dataset_test/` | 输入数据 | 生产和轻量测试 CSV |
| `cleanPy/`, `cleanPy_test/` | 清洗脚本 | 清洗 ratings 数据 |
| `initializeSQL/`, `initializeSQL_test/` | 源表 schema | Spark SQL 表结构 |
| `prepareData/`, `prepareData_test/` | 准备 SQL | 数据检查和视图创建 |
| `jobSQL/`, `jobSQL_test/` | 分析 SQL | 批处理分析任务 |
| `ci/run_spark_sql_smoke.py` | CI smoke test | 调用 `spark-submit` 验证轻量数据集 |
| `test/verify_results.sh` | 结果验证 | 调用 Scala runner 的 `--validate` 模式 |
