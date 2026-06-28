# LuckyAnJun 答辩演示清单

> 适用场景：老师要求“批处理程序展示代码和结果即可；实时处理程序需要现场演示一下”。

## 1. 展示策略

批处理部分不建议现场从头跑完整 pipeline。批处理涉及 Hive/Spark 离线计算，耗时和环境依赖较多，答辩时重点展示：

- 分析代码 / SQL 写在哪里
- 结果表已经写入 MySQL
- 网站报告能动态读取结果表生成图表

实时部分需要现场演示。建议提前把 ZooKeeper、Kafka、Spark Streaming 和页面服务启动好，现场只演示“回放数据 -> Spark 消费 -> MySQL / JSON 更新 -> 页面刷新变化”。

## 2. 批处理展示材料

### 2.1 #15 用户行为整体漏斗概况

代码入口：

- `jobSQL/06_luckyanjun_funnel_conversion.sql`
- `SparkMain/src/main/java/org/example/analysis/UserBehaviorFunnelJob.scala`

结果表：

- `lb_funnel_overall`
- `lb_funnel_overall_stage`（图表展示用，由整体漏斗宽表重排为三行）

网站报告：

- `报告 #15`

讲解重点：

- `pv` 表示浏览，`fav` 和 `cart` 合并为意向行为，`buy` 表示购买。
- 整体浏览用户 55611，意向用户 48831，购买用户 38019。
- 报表只保留整体漏斗概况，图表横轴是浏览用户、意向用户、购买用户三个阶段。
- 转化率在文字说明中讲，图表只画阶段用户数，避免宽表多指标柱状图看不清。

### 2.2 #16 逐小时流量与购买比例

代码入口：

- `jobSQL/07_luckyanjun_time_peak.sql`
- `SparkMain/src/main/java/org/example/analysis/UserBehaviorTimePeakJob.scala`

结果表：

- `lb_time_hour_distribution`

网站报告：

- `报告 #16`

讲解重点：

- 报表只保留 `lb_time_hour_distribution`，横轴按 0-23 点顺序展示。
- 图表只展示 `pv` 和 `hourly_buy_rate`，避免多个指标混在一起。
- 21 点 PV 最高，22 点第二，20 点第三，晚间 20-22 点是流量高峰。

### 2.3 #17 热门类目 Top20

代码入口：

- `jobSQL/08_luckyanjun_category_item_efficiency.sql`
- `SparkMain/src/main/java/org/example/analysis/UserBehaviorCategoryItemJob.scala`

结果表：

- `lb_category_topn`

网站报告：

- `报告 #17`

讲解重点：

- UserBehavior 数据没有金额、价格和商品名，所以只分析行为热度和行为转化，不做 GMV。
- 报表只保留热门类目 Top20，横轴使用脱敏后的 `category_id`。
- 图表只展示 `pv_cnt` 和 `category_conversion_rate`，避免报表过乱。
- 类目 4756105 的 PV 最高，为 278528，转化率为 5.78%；类目 4145813 的 PV 第二，为 184965，转化率为 7.77%。

### 2.4 #18 活跃天数分布

代码入口：

- `jobSQL/09_luckyanjun_user_segment_retention.sql`
- `SparkMain/src/main/java/org/example/analysis/UserBehaviorSegmentRetentionJob.scala`

结果表：

- `lb_user_active_day_distribution`

网站报告：

- `报告 #18`

讲解重点：

- 报表只保留“二、活跃天数分布”，横轴 `active_days` 按 1-9 递增展示。
- 活跃 9 天用户最多，为 14570 人，占 26.09%；活跃 8 天用户为 11667 人，占 20.89%。
- 重点说明活跃天数越高，平均行为数和平均购买次数整体更高。

## 3. 批处理结果核验命令

在能访问 MySQL 的机器上执行，只展示表数量即可：

```bash
mysql -h 47.104.27.184 -P 3306 -u test -p test_db -e "
SELECT 'lb_funnel_overall' AS table_name, COUNT(*) AS rows_cnt FROM lb_funnel_overall
UNION ALL SELECT 'lb_funnel_overall_stage', COUNT(*) FROM lb_funnel_overall_stage
UNION ALL SELECT 'lb_time_hour_distribution', COUNT(*) FROM lb_time_hour_distribution
UNION ALL SELECT 'lb_category_topn', COUNT(*) FROM lb_category_topn
UNION ALL SELECT 'lb_user_active_day_distribution', COUNT(*) FROM lb_user_active_day_distribution;
"
```

网站展示：

```text
http://47.104.27.184:8317/hivehbase
```

登录后进入报告展示页，选择 `报告 #15`、`报告 #16`、`报告 #17`、`报告 #18`。

## 4. 实时演示准备

实时分析对应：

- issue：`#19`
- Spark 代码：`SparkMain/src/main/scala/org/bigdata/streaming/UserBehaviorRealtimeJob.scala`
- Kafka 回放代码：`SparkMain/src/main/scala/org/bigdata/streaming/UserBehaviorReplayProducer.scala`
- 启动脚本：`start_user_behavior_streaming.sh`
- 远端 MySQL 启动脚本：`start_user_behavior_streaming_remote.sh`
- 回放脚本：`replay_user_behavior.sh`
- 页面：`web/luckyanjun_realtime.html`
- 网站报告：`报告 #19`

### 4.1 推荐现场演示方式

现场分两个终端。为了让页面变化明显，建议不要直接用很小的
`dataset_test/UserBehavior.csv`，而是现场生成一份按时间递增的 demo CSV。
这样 Spark 每个 micro-batch 都会处理新的 5 分钟窗口，页面上的窗口时间、
PV、转化率、热门类目和热门商品都会持续变化。

终端 A：启动实时任务。

```bash
cd SparkMain-LuckyAnJun

export MYSQL_HOST=47.104.27.184
export MYSQL_PORT=3306
export MYSQL_USER=test
export MYSQL_DATABASE=test_db
export MYSQL_CREATE_DATABASE=false
read -s MYSQL_PASSWORD
export MYSQL_PASSWORD

export KAFKA_USER_BEHAVIOR_TOPIC=taobao_behavior_demo
export KAFKA_STARTING_OFFSETS=latest
export USER_BEHAVIOR_CHECKPOINT=/tmp/spark/checkpoints/luckyanjun_user_behavior_demo_$(date +%Y%m%d_%H%M%S)
export STREAMING_TRIGGER_INTERVAL="2 seconds"
export REALTIME_WEB_PORT=18080

bash start_user_behavior_streaming.sh
```

看到以下信息后，说明实时任务已启动：

```text
[INFO] Starting Spark Structured Streaming
[STARTED] LuckyAnJun realtime analysis
```

终端 B：生成并回放更直观的 demo 数据。

```bash
cd SparkMain-LuckyAnJun

python3 - <<'PY'
import time

base = int(time.time()) // 300 * 300
behaviors = ["pv", "pv", "pv", "cart", "fav", "buy"]
rows = []

for window_no in range(18):
    window_start = base + window_no * 300
    for i in range(40):
        user_id = 100000 + window_no * 100 + i
        item_id = 200000 + (window_no % 8) * 10 + (i % 10)
        category_id = 500 + (window_no % 6)
        behavior = behaviors[(i + window_no) % len(behaviors)]
        event_ts = window_start + (i % 240)
        rows.append(f"{user_id},{item_id},{category_id},{behavior},{event_ts}")

path = "/tmp/UserBehavior_realtime_demo.csv"
with open(path, "w", encoding="utf-8") as f:
    f.write("\n".join(rows) + "\n")
print(path, len(rows), "rows")
PY

bash replay_user_behavior.sh \
  /tmp/UserBehavior_realtime_demo.csv \
  taobao_behavior_demo \
  localhost:9092 \
  30 \
  0
```

第 4 个参数 `30` 表示每 30 毫秒回放一条消息。18 个连续窗口、720 条
消息大约 20 多秒回放完，足够现场看到页面连续刷新。

### 4.2 页面演示

浏览器打开实时页面：

```text
http://虚拟机IP:18080/luckyanjun_realtime.html
```

展示点：

- 页面每 5 秒刷新一次。
- 指标包括 PV、近似 UV、收藏数、加购数、购买数、购买用户数和浏览到购买转化率。
- 下方显示热门类目 Top10 和热门商品 Top10。
- alert 区域展示 `normal`、`low_traffic`、`traffic_spike`、`traffic_drop` 或 `low_conversion` 等异常状态。

### 4.3 MySQL 实时结果核验

可以现场补充查一条最新窗口：

```bash
mysql -h 47.104.27.184 -P 3306 -u test -p test_db -e "
SELECT window_start, window_end, pv, approx_uv, fav_cnt, cart_cnt, buy_cnt, buy_uv, pv_to_buy_rate, alert_type
FROM lb_realtime_window_metrics
ORDER BY updated_at DESC
LIMIT 5;
"
```

也可以查实时 Top 表：

```bash
mysql -h 47.104.27.184 -P 3306 -u test -p test_db -e "
SELECT * FROM lb_realtime_category_top10
ORDER BY window_start DESC, rank_no
LIMIT 10;
SELECT * FROM lb_realtime_item_top10
ORDER BY window_start DESC, rank_no
LIMIT 10;
"
```

## 5. 答辩讲解词

批处理可以这样说：

> 批处理部分我做了四个离线分析方向。数据先经过清洗和派生时间字段，再写入 Hive 明细表；之后通过 Spark SQL 或 Scala Spark 作业生成漏斗、时段、热门类目、活跃天数等结果表，并导出到 MySQL。网站报告不是写死截图，而是根据 MySQL 结果表动态生成图表。这里我展示代码和 MySQL 结果表，以及网站上的 15 到 18 号报告。

实时处理可以这样说：

> 实时部分我用历史 UserBehavior 日志做实时回放模拟。回放程序把每条用户行为写入 Kafka topic，Spark Structured Streaming 消费 Kafka 数据，按 5 分钟事件时间窗口计算 PV、UV、收藏、加购、购买、转化率和热门类目商品，并把结果写入 MySQL，同时生成页面读取的 JSON 快照。现在我现场启动回放，可以看到页面指标随 micro-batch 更新。

## 6. 现场风险和兜底

- 如果现场网络连不上远端 MySQL，就展示本地实时页面和 `web/data/realtime.json` 更新。
- 如果 Spark 启动太慢，提前启动终端 A，现场只运行终端 B 的回放命令。
- 如果 Kafka topic 里有旧数据影响展示，设置新的 `USER_BEHAVIOR_CHECKPOINT`，并保证 `KAFKA_STARTING_OFFSETS=latest`，先启动流任务再回放。
- 不要在投屏里展示明文数据库密码；用 `read -s MYSQL_PASSWORD` 输入。
