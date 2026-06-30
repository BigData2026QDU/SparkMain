# 文件索引

## 根目录

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| Architecture.md | 架构文档 | 描述系统设计和技术架构 |
| README.md | 项目说明 | 项目入口文档 |
| File_Index.md | 文件索引 | 代码库文件清单 |
| main_pipeline.sh | Linux流水线脚本 | 自动化数据处理流水线 |
| main_pipeline.bat | Windows流水线脚本 | 自动化数据处理流水线 |
| main_pipeline_test.sh | Linux测试流水线脚本 | 使用轻量测试数据验证流水线 |
| replay_user_behavior.sh | Kafka回放脚本 | 回放淘宝用户行为测试数据 |
| start_user_behavior_streaming.sh | 实时分析启动脚本 | 启动 LuckyAnJun 用户行为实时统计 |
| build_user_behavior_realtime.sh | Scala构建脚本 | 按虚拟机 Spark/Scala 版本编译 issue #19 |
| test_user_behavior_realtime.sh | Scala测试脚本 | 执行 5 分钟窗口与热门对象 smoke test |
| export_user_behavior_mysql.sh | 离线结果导出脚本 | 将 17 张 Hive 汇总表原子发布到 MySQL |
| start_user_behavior_streaming_remote.sh | 远程实时启动脚本 | 从受保护凭据文件读取密码并写入共享 MySQL |
| initializeSQL/02_luckyanjun_realtime_mysql.sql | MySQL初始化 | 创建实时窗口、Blog 最近窗口和幂等批次表 |
| .gitignore | Git忽略配置 | 忽略IDE、构建产物等 |
| .gitmodules | Git submodule配置 | AGENTS submodule配置 |

## LuckyAnJun UserBehavior 模块

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| cleanPy/clean_user_behavior.py | 生产清洗脚本 | 清洗 `dataset/UserBehavior.csv` |
| cleanPy_test/clean_user_behavior.py | 测试清洗脚本 | 清洗 KB 级测试数据 |
| dataset_test/UserBehavior.csv | 轻量测试数据 | 用于测试流水线和 CI |
| jobSQL/06_luckyanjun_funnel_conversion.sql | 离线报表 | 严格同商品路径漏斗 |
| jobSQL/07_luckyanjun_time_peak.sql | 离线报表 | 逐小时流量与购买比例 |
| jobSQL/08_luckyanjun_category_item_efficiency.sql | 离线报表 | 热门类目 Top20 |
| jobSQL/09_luckyanjun_user_segment_retention.sql | 离线报表 | 用户活跃天数分布 |
| docs/luckyanjun_userbehavior_data_dictionary.md | 数据字典 | 字段、清洗规则和边界说明 |
| docs/luckyanjun_rawdata_dictionary.md | 原始数据字典 | 原始数据集字段定义、质量报告与分布统计 |
| docs/luckyanjun_dataset_statistics.md | 数据集统计报告 | 全量数据集统计（原始/截断/测试/清洗/结果表） |
| docs/luckyanjun_userbehavior_runbook.md | 运行说明 | 离线、测试和实时运行步骤 |
| docs/luckyanjun_mysql_contract.md | MySQL数据契约 | 前后端表名、字段、行数和接口映射 |
| docs/luckyanjun_blog_report_issue19_realtime.sql | Blog实时报表部署 | 仅更新报告 #19 的文案、图表查询和实时标记 |
| docs/adr/2026-06-30-blog-realtime-data-contract.md | 架构决策 | 记录 Blog 实时公开表、刷新方式和回滚方案 |
| SparkMain/src/main/scala/org/bigdata/export/UserBehaviorMySQLExportJob.scala | Scala导出作业 | Hive 汇总结果经 staging 校验后发布到 MySQL |
| web/luckyanjun_dashboard.html | 动态页面骨架 | 5 秒刷新接口数据 |
| generate_user_behavior_realtime_demo.py | 实时演示数据 | 生成最近 12 个连续窗口和明显的流量波动 |
| SparkMain/src/main/scala/org/bigdata/streaming/UserBehaviorRealtimeJob.scala | Scala实时作业 | Kafka 5 分钟窗口聚合并写入 MySQL |
| SparkMain/src/main/scala/org/bigdata/streaming/UserBehaviorReplayProducer.scala | Scala回放器 | 将历史 CSV 逐条发送到 Kafka |
| SparkMain/src/main/java/org/example/analysis/UserBehaviorCleanJob.scala | Scala Spark 清洗作业 | 清洗 `UserBehavior.csv` 并生成 Hive 明细表 |
| SparkMain/src/main/java/org/example/analysis/UserBehaviorFunnelJob.scala | Scala Spark 漏斗作业 | 生成 #15 严格同商品路径漏斗 |
| SparkMain/src/main/java/org/example/analysis/UserBehaviorTimePeakJob.scala | Scala Spark 时段分析作业 | 生成 #16 逐小时流量与购买比例 |
| SparkMain/src/main/java/org/example/analysis/UserBehaviorCategoryItemJob.scala | Scala Spark 类目作业 | 生成 #17 热门类目 Top20 |
| SparkMain/src/main/java/org/example/analysis/UserBehaviorSegmentRetentionJob.scala | Scala Spark 活跃度作业 | 生成 #18 用户活跃天数分布 |

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
