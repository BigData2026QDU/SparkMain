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
| `lb_funnel_item_path_stage` | 3 | 同一用户-商品按时间顺序推进的严格路径漏斗 |
| `lb_time_hour_distribution` | 24 | 24 小时流量与购买分布 |
| `lb_category_topn` | 20 | 热门类目 Top20 |
| `lb_user_active_day_distribution` | 9 | 用户活跃天数分布 |

离线导出器默认只发布以上 4 张最终报表表，不再生成或发布中间分析表。

## 实时公开表

### `lb_realtime_window_metrics`

一行对应一个 5 分钟事件时间窗口。

| 字段 | 含义 |
|---|---|
| `window_start`, `window_end` | 历史回放数据的事件时间窗口 |
| `pv` | 页面浏览次数 |
| `fav_cnt`, `cart_cnt` | 收藏数与加购数 |
| `buy_cnt` | 购买行为次数 |
| `alert_type`, `alert_message` | 流量或转化异常 |
| `updated_at` | Spark 最近更新时间 |

### `lb_realtime_blog_metrics`

Blog 报告 #19 的公开实时表，只保留最近 12 个窗口并按窗口时间升序排列。
字段包括 `window_label`, `pv`, `fav_cnt`, `cart_cnt`, `buy_cnt` 和
`alert_level`。异常等级定义为：

- `0`：正常
- `1`：低流量或低转化提醒
- `2`：流量突增或流量骤降

## 实时内部表

- `lb_realtime_batches`

该表记录 checkpoint 对应的 micro-batch，防止同一批数据重复写入。

## 推荐接口映射

| 页面模块 | 查询表 |
|---|---|
| 严格同商品路径漏斗 | `lb_funnel_item_path_stage` |
| 逐小时流量与购买比例 | `lb_time_hour_distribution` |
| 热门类目 Top20 | `lb_category_topn` |
| 用户活跃天数分布 | `lb_user_active_day_distribution` |
| 实时指标 | `lb_realtime_blog_metrics` 最近 12 个窗口 |

实时任务是历史日志回放模拟。前端应同时显示事件窗口和更新时间，不能把
2017 年事件时间描述为当前真实淘宝流量。
