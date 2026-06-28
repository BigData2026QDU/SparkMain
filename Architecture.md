# 系统架构

## 1. 整体架构

SparkMain 负责 MovieLens 25M 数据的 Spark 分析计算，包含离线批处理和实时统计两条链路。结果默认写入本地 Parquet，只有显式配置环境变量时才写入 MySQL。

```text
MovieLens 25M CSV
        |
        v
Python 下载/截断/清洗脚本
        |
        v
cleanedDataset/ + dataset/ 回退
        |
        +-------------------------------+
        |                               |
        v                               v
Spark 离线分析                    Kafka 评分流
        |                               |
        v                               v
output/ratings                    Spark Structured Streaming
output/genres                          |
output/time                            v
output/users                      output/realtime_stats
        |                               |
        +---------------+---------------+
                        |
                        v
       可选 MySQL 输出（显式启用且环境变量完整）
```

## 2. 核心模块

### 2.1 数据准备模块

- `dataset/download_movielens.py` 下载并解压 MovieLens 25M。
- `truncate_file.py` 可按大小截断大 CSV，便于本地实验。
- `cleanPy/clean_ratings.py` 清洗评分数据并输出到 `cleanedDataset/`。

### 2.2 离线分析模块

包路径：`org.bigdata.analysis`

- `AnalyzeRatings`：基于 `ratings.csv` 和 `movies.csv` 输出电影评分质量报告。
- `AnalyzeGenres`：展开 `genres` 字段，输出类型热度和评分占比报告。
- `AnalyzeTime`：使用评分时间戳按月份输出评分趋势报告。
- `AnalyzeUsers`：按评分活跃度和评分倾向输出用户行为分群报告。
- `MovieLensAnalysisSupport`：统一处理命令行参数、CSV 读取、结果写出和可选 MySQL 导出。

### 2.3 实时统计模块

包路径：`org.bigdata.streaming`

- `RealtimeWorkflow` 从 Kafka topic 读取评分 JSON。
- 解析字段：`userId`、`movieId`、`rating`、`timestamp`。
- 按 1 分钟窗口和电影聚合评分数、均分、最高分和最低分。
- 输出到 `output/realtime_stats`，并在环境变量完整时追加写入 MySQL。

### 2.4 MySQL 输出模块

包路径：`org.bigdata.utils`

- `MySQLExporter` 封装 Spark JDBC 写入。
- `MySQLExportConfig` 负责读取环境变量和 fail-closed 校验。

MySQL 默认关闭。共享连接变量为：

- `MYSQL_JDBC_URL`
- `MYSQL_USER`
- `MYSQL_PASSWORD`

离线批处理必须提供 `OFFLINE_MYSQL_ENABLED=true`，并分别配置 `MYSQL_TABLE_RATINGS`、`MYSQL_TABLE_GENRES`、`MYSQL_TABLE_TIME`、`MYSQL_TABLE_USERS`。离线任务不会读取通用 `MYSQL_TABLE`，避免四个不同 schema 的报告写入同一张表。

实时统计继续使用 `MYSQL_ENABLED=true` 和 `MYSQL_TABLE`。

### 2.5 运行包模块

- `SparkMain/build.sbt` 提供 `stageDistribution` 任务，生成免 sbt 的运行目录。
- `release/bin/sparkmain` 是发布包入口，按 allowlist 解析本地 env 文件并调用 `spark-submit`，不会执行 env 文件中的 shell 代码。
- `release/conf/sparkmain-env.example` 只包含占位配置，真实 `.env` 或 `conf/sparkmain.env` 不提交。
- `.github/workflows/pipeline-validation.yml` 在静态检查和 sbt 测试通过后生成压缩运行包；永久发布只允许 `refs/tags/v*`，手动发布必须在同一标签 ref 上运行且 `publish_version` 匹配。

## 3. 技术选型

| 技术 | 版本 | 说明 |
|------|------|------|
| Java | 17 | 与课程规范和 Spark 3.5.x 兼容 |
| Scala | 2.12.18 | Spark 3.5.0 官方二进制版本 |
| Spark | 3.5.0 | 批处理、Structured Streaming、DataFrame API |
| Kafka | 3.6.0 | 实时评分事件输入 |
| MySQL Connector/J | 8.0.33 | 可选 JDBC 输出 |
| ScalaTest | 3.2.17 | 本地 Spark 转换测试 |
| GitHub Packages | Maven | `v*` 标签发布时发布应用 JAR |

## 4. 数据流向

离线链路：

```text
dataset/*.csv -> cleanPy -> cleanedDataset/ratings.csv
dataset/movies.csv -> Spark 离线任务 -> output/<task> -> 可选离线 MySQL
```

实时链路：

```text
Kafka ratings topic -> RealtimeWorkflow -> output/realtime_stats -> 可选实时 MySQL
```

## 5. 安全和边界

- 仓库不提交 MovieLens 大 CSV、`.env`、密码或真实数据库地址。
- MySQL 写入默认禁用，缺少必要环境变量时不会退回默认数据库。
- 脚本和应用日志不打印完整 JDBC URL。
- CI 中 `MYSQL_ENABLED=false`、`OFFLINE_MYSQL_ENABLED=false`，不会执行真实 MySQL 写入。
- 发布包从 allowlist staging 目录生成，排除 `.git`、`.github`、`.codex`、真实 env 文件、输出目录和大数据 CSV。
- Release/Packages 发布 job 与验证/打包 job 分离，只在 job 级别授予写权限。
- 本仓库不实现可视化，Web 项目负责展示分析结果。
