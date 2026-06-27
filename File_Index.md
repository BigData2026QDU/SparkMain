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
| initializeSQL/02_luckyanjun_realtime_mysql.sql | MySQL初始化 | 创建实时窗口、Top10 和幂等批次表 |
| .gitignore | Git忽略配置 | 忽略IDE、构建产物等 |
| .gitmodules | Git submodule配置 | AGENTS submodule配置 |

## LuckyAnJun UserBehavior 模块

| 文件路径 | 作用 | 说明 |
|---------|------|------|
| cleanPy/clean_user_behavior.py | 生产清洗脚本 | 清洗 `dataset/UserBehavior.csv` |
| cleanPy_test/clean_user_behavior.py | 测试清洗脚本 | 清洗 KB 级测试数据 |
| dataset_test/UserBehavior.csv | 轻量测试数据 | 用于测试流水线和 CI |
| jobSQL/06_luckyanjun_funnel_conversion.sql | 离线报表 | 转化漏斗与流失分析 |
| jobSQL/07_luckyanjun_time_peak.sql | 离线报表 | 时段流量与购买高峰 |
| jobSQL/08_luckyanjun_category_item_efficiency.sql | 离线报表 | 类目和商品转化效率 |
| jobSQL/09_luckyanjun_user_segment_retention.sql | 离线报表 | 用户分层、留存与短期复购 |
| docs/luckyanjun_userbehavior_data_dictionary.md | 数据字典 | 字段、清洗规则和边界说明 |
| docs/luckyanjun_userbehavior_runbook.md | 运行说明 | 离线、测试和实时运行步骤 |
| web/luckyanjun_dashboard.html | 动态页面骨架 | 5 秒刷新接口数据 |
| web/luckyanjun_realtime.html | 实时监控页面 | 每 5 秒刷新窗口指标、Top10 和预警 |
| SparkMain/src/main/scala/org/bigdata/streaming/UserBehaviorRealtimeJob.scala | Scala实时作业 | Kafka 5 分钟窗口聚合并写入 MySQL |
| SparkMain/src/main/scala/org/bigdata/streaming/UserBehaviorReplayProducer.scala | Scala回放器 | 将历史 CSV 逐条发送到 Kafka |
| SparkMain/src/main/java/org/example/analysis/UserBehaviorCleanJob.scala | Scala Spark 清洗作业 | 清洗 `UserBehavior.csv` 并生成 Hive 明细表 |
| SparkMain/src/main/java/org/example/analysis/UserBehaviorFunnelJob.scala | Scala Spark 漏斗作业 | 生成 #15 整体和每日漏斗结果表 |
| SparkMain/src/main/java/org/example/analysis/UserBehaviorTimePeakJob.scala | Scala Spark 时段分析作业 | 生成 #16 日期小时、24 小时分布、星期小时热力图和高低转化时段结果表 |
| SparkMain/src/main/java/org/example/analysis/UserBehaviorCategoryItemJob.scala | Scala Spark 商品类目作业 | 生成 #17 类目/商品 TopN、转化排名、低转化类目和长尾贡献结果表 |
| SparkMain/src/main/java/org/example/analysis/UserBehaviorSegmentRetentionJob.scala | Scala Spark 用户分层作业 | 生成 #18 用户分层占比、活跃分布、留存热力图和复购深度对比结果表 |

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
