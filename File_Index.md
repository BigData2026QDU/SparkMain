# 文件索引

## 根目录

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| `README.md` | 项目入口文档 | 说明数据集、构建、运行、MySQL 配置和验证命令 |
| `Architecture.md` | 架构文档 | 描述离线分析、实时统计和 MySQL 输出边界 |
| `File_Index.md` | 文件索引 | 记录主要文件职责 |
| `.gitignore` | Git 忽略配置 | 忽略构建产物、数据输出、Python 缓存和 `.codex/` |
| `.gitmodules` | Submodule 配置 | AGENTS 规范子模块 |
| `.github/workflows/pipeline-validation.yml` | CI 验证和发布流水线 | 保留静态检查/sbt 测试，测试后生成运行包，只从匹配的 `v*` 标签发布 Release/Packages |
| `main_pipeline_new.sh` | 主流水线脚本 | 批处理模式运行四个离线 Spark 任务，stream 模式启动实时任务；安全解析本地 env 文件并支持 `SPARKMAIN_JAR` |
| `main_pipeline_test.sh` | 测试流水线脚本 | 使用轻量测试数据和 Hive/HDFS 测试流程 |
| `truncate_file.py` | 数据截断脚本 | 对大 CSV 做本地实验用截断 |
| `Workflow.md` | 工作流文档 | 项目流程说明 |

## SparkMain/ 构建目录

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| `SparkMain/build.sbt` | sbt 构建配置 | Scala 2.12.18、Spark 3.5.0、Kafka、MySQL、ScalaTest 依赖，支持版本覆盖、GitHub Packages 元数据和 `stageDistribution` |
| `SparkMain/project/build.properties` | sbt 版本配置 | 固定 sbt 启动版本 |

## release/ 运行包模板

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| `release/bin/sparkmain` | 发布包入口脚本 | 安全解析 `.env`、`sparkmain.env` 或 `conf/sparkmain.env`，解析运行包 JAR 并调用 `spark-submit` |
| `release/conf/sparkmain-env.example` | 本地环境模板 | 仅包含占位符和默认禁用 MySQL 的配置，复制为真实 env 文件后本地填写 |
| `release/README.md` | 运行包说明 | 说明解压后运行、env 文件和 MySQL 安全边界 |

## SparkMain/src/main/scala/org/bigdata/

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| `SparkMain/src/main/scala/org/bigdata/Main.scala` | 统一入口 | 注册 `ratings`、`genres`、`time`、`users`、`realtime` 和 `help` |

### analysis/

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| `SparkMain/src/main/scala/org/bigdata/analysis/MovieLensAnalysisSupport.scala` | 离线任务公共工具 | 解析输入/输出参数、读取 MovieLens CSV、写 Parquet/CSV、触发可选 MySQL 导出 |
| `SparkMain/src/main/scala/org/bigdata/analysis/AnalyzeRatings.scala` | 评分质量分析 | 输出电影评分数、均分、最高/最低分、评分标准差 |
| `SparkMain/src/main/scala/org/bigdata/analysis/AnalyzeGenres.scala` | 类型热度分析 | 展开 `genres`，统计类型评分量、电影数、用户数和评分占比 |
| `SparkMain/src/main/scala/org/bigdata/analysis/AnalyzeTime.scala` | 时间趋势分析 | 基于评分时间戳按月份统计趋势 |
| `SparkMain/src/main/scala/org/bigdata/analysis/AnalyzeUsers.scala` | 用户行为分析 | 按活跃度和评分倾向汇总用户分群 |

### streaming/

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| `SparkMain/src/main/scala/org/bigdata/streaming/RealtimeWorkflow.scala` | 实时统计任务 | Kafka JSON 评分流解析、1 分钟窗口聚合、Parquet 输出和可选 MySQL 追加写入 |

### utils/

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| `SparkMain/src/main/scala/org/bigdata/utils/MySQLExporter.scala` | JDBC 写入工具 | 封装 Spark DataFrame 写 MySQL |
| `SparkMain/src/main/scala/org/bigdata/utils/MySQLExportConfig.scala` | MySQL 配置门禁 | 从环境变量读取配置，默认禁用，启用时校验 URL、用户、密码、表名 |

## SparkMain/src/test/scala/org/bigdata/

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| `SparkMain/src/test/scala/org/bigdata/analysis/AnalysisReportsSpec.scala` | 离线分析测试 | 使用内存 DataFrame 验证四个报告转换 |
| `SparkMain/src/test/scala/org/bigdata/streaming/RealtimeWorkflowSpec.scala` | 实时统计测试 | 验证流式聚合和 MySQL 配置 fail-closed 行为 |

## 数据与脚本目录

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| `dataset/download_movielens.py` | 数据下载脚本 | 下载 GroupLens MovieLens 25M 并解压 CSV 到 `dataset/` |
| `dataset_test/` | 轻量测试数据 | 小体积 CSV 样例，不替代 MovieLens 25M 正式数据 |
| `cleanPy/clean_ratings.py` | 生产清洗脚本 | 清洗 `dataset/ratings.csv` 到 `cleanedDataset/ratings.csv` |
| `cleanPy_test/clean_ratings.py` | 测试清洗脚本 | 清洗 `dataset_test/ratings.csv` 到 `cleanedDataset_test/ratings.csv` |
| `test/verify_results.sh` | 结构验证脚本 | 检查实际 `org/bigdata` 路径和关键 Scala 文件是否存在 |

## AGENTS/ 规范目录

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| `AGENTS/AGENTS.md` | AI 助手入口 | 项目规范导航 |
| `AGENTS/PROJECT/PROJECT.md` | 通用规范 | 技术栈、目录结构、文档要求 |
| `AGENTS/PROJECT/BACKEND.md` | 后端规范 | Java/Spark/测试要求 |
| `AGENTS/PROJECT/SPARK.md` | Spark 规范 | Spark 流水线和 MySQL 输出规范 |
| `AGENTS/PROJECT/TESTING.md` | 测试规范 | 测试框架和 CI 要求 |
