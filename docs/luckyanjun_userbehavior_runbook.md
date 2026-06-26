# LuckyAnJun UserBehavior 运行说明

## 离线分析

1. 将生产数据 `UserBehavior.csv` 放入 `dataset/`。
2. 在 CentOS 虚拟机启动 HDFS、Hive metastore、HiveServer2 和 Spark。
3. 在仓库根目录执行:

```bash
bash main_pipeline.sh
```

流水线会生成:

- `dwd_user_behavior_clean`
- `lb_funnel_overall`, `lb_funnel_daily`
- `lb_time_hourly_behavior`, `lb_time_weekday_hour_heatmap`
- `lb_category_efficiency`, `lb_item_efficiency`, `lb_item_long_tail`
- `lb_user_segments`, `lb_user_segment_summary`, `lb_user_active_day_distribution`, `lb_user_retention`, `lb_repurchase_behavior_depth`

## 测试验证

```bash
bash main_pipeline_test.sh
bash test/verify_results.sh
```

测试数据位于 `dataset_test/UserBehavior.csv`，规模保持在 KB 级，适合 GitHub CI 做静态校验，也适合本地/虚拟机快速跑通 Hive 流程。

## 实时分析

启动 Kafka 后，先启动 Spark Structured Streaming:

```bash
bash start_user_behavior_streaming.sh
```

另一个终端执行历史日志回放:

```bash
cd SparkMain
mvn -q -DskipTests package
java -cp target/spark-streaming-kafka-1.0.0.jar org.example.streaming.UserBehaviorProducer ../dataset_test/UserBehavior.csv taobao_behavior localhost:9092 200
```

答辩时应表述为“历史用户行为日志实时回放模拟”，不要表述为接入淘宝官方实时流。
