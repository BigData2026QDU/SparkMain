# LuckyAnJun UserBehavior 运行说明

## 离线清洗与入库

1. 将淘宝用户行为生产数据放入 `dataset/UserBehavior.csv`。
2. 在 CentOS 虚拟机启动 HDFS、Hive metastore、HiveServer2 和 Spark。
3. 在仓库根目录执行：

```bash
bash main_pipeline.sh
```

如果当前 Hive 的 Spark execution engine 缺少 Scala 运行类，可在同一套代码下临时切换为 MR 执行：

```bash
HIVE_EXECUTION_ENGINE=mr bash main_pipeline.sh
```

流水线会依次执行：

- `truncate_file.py`：按行截取约 200 MB 原始 CSV 到 `truncatedDataset/`，作为 issue #14 的生产抽样策略。
- `cleanPy/clean_user_behavior.py`：过滤异常记录，派生时间字段，输出 `cleanedDataset/user_behavior.csv`。
- HDFS 上传：按 CSV 文件名创建表目录，例如 `/user/hive/bigdata_ana/user_behavior/`。
- `initializeSQL/01_create_tables.sql`：创建 Hive 外部表 `user_behavior`。
- `prepareData/01_load_data.sql`：生成 `dwd_user_behavior_clean` 和 `v_user_item_day_flags`。
- `jobSQL/`：执行后续 LuckyAnJun 个人离线分析报表。

## 关键产物

- `cleanedDataset/user_behavior.csv`
- `user_behavior`
- `dwd_user_behavior_clean`
- `v_user_item_day_flags`
- `lb_funnel_overall`, `lb_funnel_daily`
- `lb_time_hourly_behavior`, `lb_time_weekday_hour_heatmap`
- `lb_category_efficiency`, `lb_item_efficiency`, `lb_item_long_tail`
- `lb_user_segments`, `lb_user_segment_summary`, `lb_user_retention`

## 测试验证

测试数据位于 `dataset_test/UserBehavior.csv`，保持 KB 级，适合 CI 和本地虚拟机快速跑通流程。

```bash
bash main_pipeline_test.sh
bash test/verify_results.sh
```

测试清洗脚本不强制 15000 行规模门槛；生产清洗脚本默认强制不少于 15000 行，并限制输出不超过 500 MB。

## 实时分析

实时模块使用历史日志回放模拟 Kafka 流，不表示接入淘宝官方实时流。

```bash
cd SparkMain
mvn -q -DskipTests package
cd ..
bash start_user_behavior_streaming.sh
bash replay_user_behavior.sh dataset_test/UserBehavior.csv taobao_behavior localhost:9092 200
```

## 答辩说明

- 本模块是 LuckyAnJun 个人独立任务，只使用 `UserBehavior.csv`。
- `buy` 表示购买行为次数，不代表订单金额或销售额。
- 后续正式分析需要提交到 CentOS 虚拟机中的 Hadoop/Hive/Spark/Kafka 环境执行；Windows 只用于编辑代码、GitHub 管理和页面访问。
