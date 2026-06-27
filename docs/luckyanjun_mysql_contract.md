# LuckyAnJun MySQL 数据契约

## 连接与职责

- 数据库：`test_db`
- 表前缀：`lb_`
- 离线来源：Hive `bigdata_ana`
- 实时来源：Kafka `taobao_behavior` + Spark Structured Streaming
- 比例字段均为 `0–1` 小数，前端展示百分比时乘以 100
- `spark_exported_at` 是离线结果发布时间
- `updated_at` 是实时窗口最后更新时间

后端只读查询正式表，不查询 `__staging`、`__backup` 或实时内部状态表。

## 离线公开表

| 表名 | 当前行数 | 用途 |
|---|---:|---|
| `lb_funnel_overall` | 1 | 整体 PV、意向、购买漏斗与流失率 |
| `lb_funnel_daily` | 9 | 每日漏斗趋势 |
| `lb_time_hourly_behavior` | 216 | 日期小时行为指标 |
| `lb_time_hour_distribution` | 24 | 24 小时流量与购买分布 |
| `lb_time_weekday_hour_heatmap` | 168 | 星期 × 小时热力图 |
| `lb_time_high_conversion_slots` | 10 | 高转化时段 Top10 |
| `lb_time_low_conversion_slots` | 10 | 低转化时段 Top10 |
| `lb_category_efficiency` | 7392 | 类目完整汇总，供筛选和详情查询 |
| `lb_category_topn` | 20 | 热门类目 Top20 |
| `lb_item_topn` | 20 | 热门商品 Top20 |
| `lb_category_conversion_rank` | 20 | 类目转化率 Top20 |
| `lb_category_low_conversion` | 20 | 低转化类目 Top20 |
| `lb_user_segment_summary` | 5 | 用户分层汇总 |
| `lb_user_active_day_distribution` | 9 | 用户活跃天数分布 |
| `lb_user_retention` | 8 | 1/3/7 日留存 |
| `lb_user_retention_heatmap` | 24 | 留存热力图长表 |
| `lb_repurchase_behavior_depth` | 2 | 复购与非复购行为深度对比 |

未导出 `lb_item_efficiency`、`lb_item_long_tail`、`lb_user_features` 和
`lb_user_segments`，避免把百万行商品明细和用户级数据放入展示数据库。

## 实时公开表

### `lb_realtime_window_metrics`

一行对应一个 5 分钟事件时间窗口。

| 字段 | 含义 |
|---|---|
| `window_start`, `window_end` | 历史回放数据的事件时间窗口 |
| `pv`, `approx_uv` | 浏览次数与浏览用户数 |
| `fav_cnt`, `cart_cnt` | 收藏数与加购数 |
| `buy_cnt`, `buy_uv` | 购买行为数与购买用户数 |
| `pv_to_buy_rate` | 购买用户数 / 浏览用户数 |
| `alert_type`, `alert_message` | 流量或转化异常 |
| `updated_at` | Spark 最近更新时间 |

### `lb_realtime_category_top10`

字段：`window_start`, `window_end`, `rank_no`, `category_id`,
`event_cnt`, `buy_cnt`。

### `lb_realtime_item_top10`

字段：`window_start`, `window_end`, `rank_no`, `item_id`,
`event_cnt`, `buy_cnt`。

## 实时内部表

以下表由 Spark 维护，后端通常不需要查询：

- `lb_realtime_batches`
- `lb_realtime_window_users`
- `lb_realtime_category_stats`
- `lb_realtime_item_stats`

## 推荐接口映射

| 页面模块 | 查询表 |
|---|---|
| 漏斗总览 | `lb_funnel_overall` |
| 漏斗趋势 | `lb_funnel_daily` |
| 时段趋势 | `lb_time_hour_distribution` |
| 星期小时热力图 | `lb_time_weekday_hour_heatmap` |
| 类目与商品榜单 | `lb_category_topn`, `lb_item_topn` |
| 用户分层 | `lb_user_segment_summary` |
| 留存分析 | `lb_user_retention`, `lb_user_retention_heatmap` |
| 实时指标 | `lb_realtime_window_metrics` 最新窗口 |
| 实时榜单 | 两张实时 Top10 表中与最新窗口相同的数据 |

实时任务是历史日志回放模拟。前端应同时显示事件窗口和更新时间，不能把
2017 年事件时间描述为当前真实淘宝流量。
