# LuckyAnJun UserBehavior 数据字典

## 数据集

- 数据文件: `UserBehavior.csv`
- 来源: 淘宝用户行为日志数据
- 个人任务: LuckyAnJun 独立分析任务
- 字段限制: 无价格、金额、订单号、商品名称、用户性别年龄和城市字段，因此不做 GMV、销售额、利润、客单价等分析

## 原始字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `user_id` | BIGINT | 脱敏用户 ID |
| `item_id` | BIGINT | 脱敏商品 ID |
| `category_id` | BIGINT | 脱敏商品类目 ID |
| `behavior_type` | STRING | 用户行为类型: `pv`, `buy`, `cart`, `fav` |
| `timestamp` | BIGINT | 秒级 Unix 时间戳 |

## 清洗后字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `event_time` | TIMESTAMP | 由 `timestamp` 转换得到的事件时间 |
| `event_date` | STRING | 事件日期，用于每日报表和留存 cohort |
| `event_hour` | INT | 事件小时，用于时段高峰分析 |
| `weekday` | INT | ISO 星期序号，1 到 7 |

## 清洗规则

- 只保留 5 列完整的原始行为记录。
- `user_id`、`item_id`、`category_id`、`timestamp` 必须能转换为整数。
- `behavior_type` 只允许 `pv`、`buy`、`cart`、`fav`。
- `timestamp` 必须大于 0。
- 开发测试使用 `dataset_test/UserBehavior.csv`，生产运行使用 `dataset/UserBehavior.csv` 经截断后清洗。

## 集群运行约束

最终分析必须在 CentOS 虚拟机中的 Hadoop/Hive/Spark/Kafka 环境执行。Windows 只负责编辑代码、管理 GitHub、触发远程命令和访问展示页面。
