-- LuckyAnJun 离线分析 #18：用户活跃天数分布与行为深度。
SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana;

DROP TABLE IF EXISTS lb_user_active_day_distribution;
CREATE EXTERNAL TABLE lb_user_active_day_distribution (
    active_days BIGINT,
    user_cnt BIGINT,
    user_rate DOUBLE,
    avg_behavior_cnt DOUBLE,
    avg_distinct_item_cnt DOUBLE,
    avg_buy_cnt DOUBLE
)
STORED AS PARQUET
LOCATION '/user/hive/bigdata_ana/lb_user_active_day_distribution';

INSERT OVERWRITE TABLE lb_user_active_day_distribution
WITH user_activity AS (
    SELECT
        user_id,
        COUNT(DISTINCT event_date) AS active_days,
        COUNT(*) AS behavior_cnt,
        COUNT(DISTINCT item_id) AS distinct_item_cnt,
        COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt
    FROM dwd_user_behavior_clean
    WHERE user_id IS NOT NULL
      AND item_id IS NOT NULL
      AND event_date IS NOT NULL
      AND event_date <> 'event_date'
      AND behavior_type IN ('pv', 'fav', 'cart', 'buy')
    GROUP BY user_id
),
total_users AS (
    SELECT COUNT(*) AS user_cnt
    FROM user_activity
)
SELECT
    active_days,
    COUNT(*) AS user_cnt,
    ROUND(COUNT(*) / CAST(MAX(total_users.user_cnt) AS DOUBLE), 4) AS user_rate,
    ROUND(AVG(behavior_cnt), 2) AS avg_behavior_cnt,
    ROUND(AVG(distinct_item_cnt), 2) AS avg_distinct_item_cnt,
    ROUND(AVG(buy_cnt), 2) AS avg_buy_cnt
FROM user_activity
CROSS JOIN total_users
GROUP BY active_days;

SELECT * FROM lb_user_active_day_distribution ORDER BY active_days;
