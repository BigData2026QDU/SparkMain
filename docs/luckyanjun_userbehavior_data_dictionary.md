# LuckyAnJun UserBehavior 数据字典

## 数据集边界

- 任务来源：GitHub issue #14 `[LuckyAnJun][个人独立分析] 数据集抽样、清洗与数据字典`
- 个人任务：LuckyAnJun 独立 Spark 分析任务，不覆盖其他组员工作
- 原始文件：`dataset/UserBehavior.csv`
- 生产抽样：流水线先使用 `truncate_file.py` 按行截取约 300 MB 原始 CSV 到 `truncatedDataset/UserBehavior.csv`，再清洗输出 `cleanedDataset/user_behavior.csv`
- 验收规模：生产清洗结果必须不少于 15000 行，且输出 CSV 不超过 500 MB
- 分析边界：该数据没有价格、订单号、商品名称、用户性别、年龄、城市等字段，因此不做 GMV、销售额、利润、客单价或用户画像分析

## 原始字段

| 字段 | 类型 | 说明 | 取值范围 |
| --- | --- | --- | --- |
| `user_id` | BIGINT | 脱敏用户 ID | 正整数 |
| `item_id` | BIGINT | 脱敏商品 ID | 正整数 |
| `category_id` | BIGINT | 脱敏商品类目 ID | 正整数 |
| `behavior_type` | STRING | 用户行为类型 | `pv`, `buy`, `cart`, `fav` |
| `timestamp` | BIGINT | 秒级 Unix 时间戳 | 2017-11-25 至 2017-12-03，北京时间 |

## 清洗后字段

| 字段 | Hive 类型 | 说明 | 业务用途 |
| --- | --- | --- | --- |
| `user_id` | BIGINT | 脱敏用户 ID | UV、用户行为路径、分层留存 |
| `item_id` | BIGINT | 脱敏商品 ID | 商品热度、商品转化效率 |
| `category_id` | BIGINT | 脱敏商品类目 ID | 类目热度、类目转化效率 |
| `behavior_type` | STRING | 行为类型 | 漏斗分析、行为结构分析 |
| `timestamp` | BIGINT | 原始秒级时间戳 | 保留原始时间依据 |
| `event_time` | TIMESTAMP | 北京时间事件时间 | 明细查询、排序、窗口分析 |
| `event_date` | STRING | 事件日期，格式 `yyyy-MM-dd` | 日报、留存 cohort |
| `event_hour` | INT | 事件小时 | 时段高峰分析 |
| `weekday` | INT | ISO 星期序号，1=周一，7=周日 | 星期热力图 |

## 清洗规则

- 只保留 5 列完整记录，列顺序必须为 `user_id,item_id,category_id,behavior_type,timestamp`。
- `user_id`、`item_id`、`category_id`、`timestamp` 必须能转换为整数，且 ID 和时间戳必须大于 0。
- `behavior_type` 转为小写后只允许 `pv`、`buy`、`cart`、`fav`。
- `timestamp` 按北京时间 UTC+8 转换为 `event_time`，并派生 `event_date`、`event_hour`、`weekday`。
- 仅保留北京时间 2017-11-25 至 2017-12-03 范围内的事件，过滤异常时间戳。
- 清洗脚本输出跳过行数和跳过原因，便于答辩说明缺失值、异常行为类型和异常时间戳处理。

## Hive 表

| 表名 | 类型 | 说明 |
| --- | --- | --- |
| `user_behavior` | 外部表 | HDFS 中清洗后 CSV 的落地表 |
| `dwd_user_behavior_clean` | 管理表 | 从外部表生成的明细宽表，后续离线分析统一读取该表 |
| `v_user_item_day_flags` | 视图 | 用户-商品-日期粒度的浏览、意向、购买标记 |

## HDFS 路径

- 生产数据库：`bigdata_ana`
- 生产 HDFS 根路径：`/user/hive/bigdata_ana`
- 明细外部表路径：`/user/hive/bigdata_ana/user_behavior`
- 测试数据库：`bigdata_ana_test`
- 测试 HDFS 根路径：`/user/hive/bigdata_ana_test`
