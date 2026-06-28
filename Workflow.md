# SparkMain 工作流

## 1. 边界

SparkMain 负责 MovieLens 数据准备、Spark 离线分析、Kafka 实时评分统计和可选 MySQL 结果输出。本仓库不包含可视化页面；结果展示由 Web 项目处理。

## 2. 技术栈

| 层次 | 技术 |
|------|------|
| 数据准备 | Python |
| 离线分析 | Spark SQL / Scala |
| 实时统计 | Spark Structured Streaming |
| 消息队列 | Apache Kafka |
| 本地结果 | Parquet / CSV |
| 可选结果库 | MySQL |

## 3. 离线批处理

默认批处理入口：

```bash
bash main_pipeline_new.sh
```

流程：

```text
dataset/ratings.csv
        |
        v
truncate_file.py
        |
        v
cleanPy/clean_ratings.py
        |
        v
cleanedDataset/ratings.csv + dataset/movies.csv
        |
        v
org.bigdata.Main
        |
        +--> ratings -> AnalyzeRatings -> output/ratings
        +--> genres  -> AnalyzeGenres  -> output/genres
        +--> time    -> AnalyzeTime    -> output/time
        +--> users   -> AnalyzeUsers   -> output/users
```

四个离线报告默认只写本地输出目录。离线 MySQL 输出必须显式设置 `OFFLINE_MYSQL_ENABLED=true`，并分别提供：

- `MYSQL_TABLE_RATINGS`
- `MYSQL_TABLE_GENRES`
- `MYSQL_TABLE_TIME`
- `MYSQL_TABLE_USERS`

离线批处理不使用通用 `MYSQL_TABLE`，避免四个报告覆盖同一张表。

## 4. 实时统计

实时入口：

```bash
bash main_pipeline_new.sh stream
```

流程：

```text
Kafka ratings topic
        |
        v
org.bigdata.Main realtime
        |
        v
org.bigdata.streaming.RealtimeWorkflow
        |
        +--> output/realtime_stats
        +--> optional MySQL table from MYSQL_TABLE
```

实时 MySQL 输出默认关闭。启用时使用 `MYSQL_ENABLED=true`、`MYSQL_JDBC_URL`、`MYSQL_USER`、`MYSQL_PASSWORD` 和 `MYSQL_TABLE`。

## 5. 当前文件结构

```text
SparkMain/
├── main_pipeline_new.sh
├── release/
│   ├── bin/sparkmain
│   ├── conf/sparkmain-env.example
│   └── README.md
├── test/verify_results.sh
├── SparkMain/
│   ├── build.sbt
│   └── src/
│       ├── main/scala/org/bigdata/
│       │   ├── Main.scala
│       │   ├── analysis/
│       │   │   ├── AnalyzeRatings.scala
│       │   │   ├── AnalyzeGenres.scala
│       │   │   ├── AnalyzeTime.scala
│       │   │   ├── AnalyzeUsers.scala
│       │   │   └── MovieLensAnalysisSupport.scala
│       │   ├── streaming/
│       │   │   └── RealtimeWorkflow.scala
│       │   └── utils/
│       │       ├── MySQLExportConfig.scala
│       │       └── MySQLExporter.scala
│       └── test/scala/org/bigdata/
│           ├── analysis/AnalysisReportsSpec.scala
│           └── streaming/RealtimeWorkflowSpec.scala
├── dataset/
├── cleanPy/
└── output/
```

## 6. 运行包和发布

CI 保留静态检查和 sbt 测试，测试通过后才执行运行包构建。运行包从显式 staging 目录生成，只包含应用 JAR、运行依赖 JAR、脚本、示例配置、文档和小型测试样例，不包含 `.git`、`.github`、`.codex`、真实 env 文件、输出目录或大数据 CSV。

普通 push、PR 和 `codex/**` 分支只生成 Actions artifact，不发布永久产物。GitHub Release 和 GitHub Packages 只允许从 `refs/tags/v*` 标签发布；手动发布必须在同一个 `v*` 标签 ref 上运行，并且 `publish_version` 必须匹配该标签。发布 job 才获得 `contents: write` 或 `packages: write` 权限。

下载运行包后：

```bash
cp conf/sparkmain-env.example conf/sparkmain.env
bash bin/sparkmain help
bash bin/sparkmain ratings dataset output/ratings
bash bin/sparkmain stream
```

本地 env 文件不会上传；默认 `MYSQL_ENABLED=false`、`OFFLINE_MYSQL_ENABLED=false`。env 文件按 SparkMain 已知 `KEY=VALUE` 配置解析，不执行 shell 代码。

## 7. 验证

安全的本地结构和语法检查：

```bash
bash -n main_pipeline_new.sh
bash -n main_pipeline_test.sh
bash -n test/verify_results.sh
bash -n release/bin/sparkmain
python -m py_compile truncate_file.py dataset/download_movielens.py cleanPy/clean_ratings.py cleanPy_test/clean_ratings.py
bash test/verify_results.sh
```

Scala 编译和单元测试需要本机安装 `sbt`：

```bash
cd SparkMain
sbt -batch clean test
```
