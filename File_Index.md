# 文件索引

## 根目录

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| Architecture.md | 架构文档 | 描述系统设计和技术架构 |
| README.md | 项目说明 | 项目入口文档 |
| File_Index.md | 文件索引 | 代码库文件清单 |
| main_pipeline.sh | Linux流水线脚本 | 自动化数据处理流水线 |
| main_pipeline.bat | Windows流水线脚本 | 自动化数据处理流水线 |
| .gitignore | Git忽略配置 | 忽略IDE、构建产物等 |
| .gitmodules | Git submodule配置 | AGENTS submodule配置 |

## SparkMain/ 目录

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| SparkMain/src/ | 源代码目录 | 项目源代码 |
| SparkMain/test/ | 测试代码目录 | 项目测试代码 |

## AGENTS/ 目录

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| AGENTS/AGENTS.md | AI助手入口文档 | 学期项目规范导航 |
| AGENTS/CLAUDE.md | Claude Code配置 | Claude Code/Codex工作配置 |
| AGENTS/PROJECT/PROJECT.md | 全项目通用规范 | 技术栈、目录结构、文档要求 |
| AGENTS/PROJECT/BACKEND.md | 后端开发规范 | Java、Spark、测试规范 |
| AGENTS/PROJECT/FRONTEND.md | 前端开发规范 | 前端开发规范 |
## Personal realtime analysis

| File path | Role | Description |
|---------|------|------|
| SparkMain/src/main/scala/org/bigdata/streaming/PersonalRealtimeRatings.scala | Personal realtime Spark task | Consumes rating events from Kafka or file stream, computes 5-minute metrics, Top movies, alerts, and the Blog overview table, then writes Parquet and optional MySQL tables. |
| run_personal_realtime.sh | Realtime validation runner | Runs the personal realtime task with guarded output cleanup, optional Spark packages/jars, and optional test table cleanup. |
| run_realtime_demo_producer.sh | Blog realtime demo producer | Continuously replays MovieLens ratings to Kafka and advances one 5-minute event window about every 4 seconds for visible Blog chart movement. |
| scripts/continuous_kafka_replay.py | Kafka replay helper | Streams CSV rows to Kafka as JSON rating events with synthetic accelerated event timestamps. |
| dataset_test/realtime_ratings/ratings_batch_1.csv | Realtime smoke input | Small CSV batch used by file-stream validation and Kafka producer replay. |
