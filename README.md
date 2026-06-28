# SparkMain

## 项目简介

SparkMain 是基于 Java 17、Scala 2.12 和 Apache Spark 3.5.0 的 MovieLens 大数据分析流水线。仓库负责离线分析与实时统计结果生成；可视化展示由 Web 项目承担，本仓库只在配置允许时把分析结果写入 MySQL。

## 数据集选择

本项目选择 GroupLens MovieLens 25M 数据集，下载脚本为 `dataset/download_movielens.py`，目标压缩包为 `ml-25m.zip`。

选择理由：

- 数据规模足够大：约 2500 万条评分、约 62000 部电影、约 162000 名用户，解压后 CSV 总体积超过普通课堂样例数据，适合 Spark 批处理。
- 字段结构稳定：`ratings.csv`、`movies.csv` 可以支撑评分质量、类型热度、时间趋势和用户行为四类不同分析。
- 版权和来源清晰：数据由 GroupLens 发布，便于复现实验。

大 CSV 文件不会提交到 Git。请通过下载脚本或手动下载后放入 `dataset/`。

```bash
python dataset/download_movielens.py
```

## 环境要求

| 组件 | 版本 | 用途 |
|------|------|------|
| JDK | 17 | Spark/Scala 运行环境 |
| Scala | 2.12 | Spark 3.5.0 二进制兼容版本 |
| Apache Spark | 3.5.0 | 离线分析与实时统计 |
| sbt | 1.9+ | 构建 Scala 项目 |
| Python | 3.8+ | 数据下载、截断和清洗脚本 |
| Kafka | 3.6+ | 实时评分流输入 |
| MySQL | 8.0+ | 可选分析结果输出 |

## 构建

```bash
cd SparkMain
sbt package
```

构建产物路径：

```text
SparkMain/target/scala-2.12/sparkmain_2.12-1.0.0.jar
```

CI 或发布构建可以覆盖版本号：

```bash
cd SparkMain
sbt -Dsparkmain.version=1.0.1 package
```

## 免 sbt 运行包

发布流水线会在测试通过后生成 `sparkmain-<version>.tar.gz`。下载并解压后，运行 Spark 任务不需要 sbt 或源码仓库；本机需要准备 Java 17、Scala 2.12、Spark 3.5.x、Kafka 3.6.x（实时任务需要）和一个本地 env 文件。

本地配置文件不会提交到仓库。复制模板后只在本机填写真实值：

```bash
cp conf/sparkmain-env.example conf/sparkmain.env
```

常用命令：

```bash
bash bin/sparkmain help
bash bin/sparkmain ratings dataset output/ratings
bash bin/sparkmain stream
bash bin/sparkmain batch
```

`bin/sparkmain` 和 `main_pipeline_new.sh` 会自动加载第一个存在的 `.env`、`sparkmain.env` 或 `conf/sparkmain.env`，并优先使用 `SPARKMAIN_JAR`；未设置时会自动寻找运行包内的 `lib/sparkmain_2.12-*.jar`。完整批处理 `batch` 会执行 Python 数据准备脚本，因此从原始 MovieLens CSV 开始跑全流程时仍需要 Python 3.8+。

本地 env 文件只支持 SparkMain 已知配置项的 `KEY=VALUE` 行；脚本不会执行 env 文件中的 shell 代码，无法识别的行会被跳过。

## 离线分析任务

四个离线任务位于 `SparkMain/src/main/scala/org/bigdata/analysis/`，统一通过 `org.bigdata.Main` 调度。默认输入目录为 `cleanedDataset/`，缺少文件时回退到 `dataset/`；默认输出为 Parquet，路径位于 `output/<task>`。

| 任务 | 命令名 | 输出目录 | 分析点 |
|------|--------|----------|--------|
| `AnalyzeRatings` | `ratings` | `output/ratings` | 电影评分质量：评分数、均分、最高/最低分、评分标准差 |
| `AnalyzeGenres` | `genres` | `output/genres` | 类型热度：展开电影类型后统计评分量、电影数、用户数、评分占比 |
| `AnalyzeTime` | `time` | `output/time` | 时间趋势：按月份统计评分量、活跃用户、被评电影和平均评分 |
| `AnalyzeUsers` | `users` | `output/users` | 用户行为分群：按评分活跃度和评分倾向汇总用户群体 |

运行单个任务：

```bash
spark-submit \
  --class org.bigdata.Main \
  --master local[*] \
  SparkMain/target/scala-2.12/sparkmain_2.12-1.0.0.jar \
  ratings cleanedDataset output/ratings
```

也可以使用参数形式：

```bash
spark-submit --class org.bigdata.Main --master local[*] \
  SparkMain/target/scala-2.12/sparkmain_2.12-1.0.0.jar \
  genres --input cleanedDataset --output output/genres --format parquet
```

运行完整批处理流水线：

```bash
python dataset/download_movielens.py
bash main_pipeline_new.sh
```

## 实时统计任务

实时任务使用 Kafka + Spark Structured Streaming 读取评分 JSON，按 1 分钟窗口和 `movieId` 统计：

- `rating_count`
- `avg_rating`
- `max_rating`
- `min_rating`

启动方式：

```bash
bash main_pipeline_new.sh stream
```

默认会把实时结果写入 `output/realtime_stats`。实时 MySQL 输出默认关闭，只有显式配置 `MYSQL_ENABLED=true` 后才会写入。

## MySQL 输出配置

所有 MySQL 写入都使用环境变量配置，代码和脚本不包含账号、密码或默认生产库地址。离线批处理和实时统计使用不同的启用开关，默认都不会写 MySQL。

推荐将本地配置写入 `.env` 或 `conf/sparkmain.env`，并从 `release/conf/sparkmain-env.example` 复制占位模板。真实 env 文件已被 `.gitignore` 忽略，不能提交或上传。

共享连接变量：

```bash
export MYSQL_JDBC_URL="jdbc:mysql://<host>:<port>/<database>?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
export MYSQL_USER="<username>"
export MYSQL_PASSWORD="<password>"
```

离线批处理必须显式启用 `OFFLINE_MYSQL_ENABLED=true`，并且四个报告分别使用任务专用表名；离线任务不会读取通用 `MYSQL_TABLE`：

```bash
export OFFLINE_MYSQL_ENABLED="true"
export MYSQL_TABLE_RATINGS="<ratings_table>"
export MYSQL_TABLE_GENRES="<genres_table>"
export MYSQL_TABLE_TIME="<time_table>"
export MYSQL_TABLE_USERS="<users_table>"
```

实时统计继续使用 `MYSQL_ENABLED=true` 和通用 `MYSQL_TABLE`：

```bash
export MYSQL_ENABLED="true"
export MYSQL_TABLE="<realtime_table>"
```

安全约束：

- 不提交 `.env`、密码、真实 JDBC URL 或截图中的凭据。
- 日志只显示 MySQL 是否启用和表名，不打印完整 JDBC URL。
- 测试不连接真实数据库。

## 测试和验证命令

静态脚本检查：

```bash
bash -n main_pipeline_new.sh
bash -n main_pipeline_test.sh
bash -n test/verify_results.sh
bash -n release/bin/sparkmain
```

Python 脚本语法检查：

```bash
python -m py_compile truncate_file.py dataset/download_movielens.py cleanPy/clean_ratings.py cleanPy_test/clean_ratings.py
```

Scala 单元测试：

```bash
cd SparkMain
sbt -batch clean test
```

## 目录结构

```text
SparkMain/
├── SparkMain/
│   ├── build.sbt
│   └── src/
│       ├── main/scala/org/bigdata/
│       │   ├── Main.scala
│       │   ├── analysis/
│       │   ├── streaming/
│       │   └── utils/
│       └── test/scala/org/bigdata/
├── dataset/
├── dataset_test/
├── cleanPy/
├── cleanPy_test/
├── output/
├── release/
│   ├── bin/sparkmain
│   ├── conf/sparkmain-env.example
│   └── README.md
├── main_pipeline_new.sh
├── main_pipeline_test.sh
├── Architecture.md
├── File_Index.md
└── README.md
```

## 许可证

本项目遵循课程项目仓库规范中的许可证要求。
