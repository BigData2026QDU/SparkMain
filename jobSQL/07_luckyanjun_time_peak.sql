-- LuckyAnJun 离线分析 #16：0-23 点流量与购买比例。
SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana;

DROP TABLE IF EXISTS lb_time_hour_distribution;
CREATE EXTERNAL TABLE lb_time_hour_distribution (
    event_hour INT,
    pv BIGINT,
    pv_uv BIGINT,
    fav_cnt BIGINT,
    cart_cnt BIGINT,
    buy_cnt BIGINT,
    buy_uv BIGINT,
    hourly_buy_rate DOUBLE
)
STORED AS PARQUET
LOCATION '/user/hive/bigdata_ana/lb_time_hour_distribution';

INSERT OVERWRITE TABLE lb_time_hour_distribution
SELECT
    event_hour,
    COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END) AS pv,
    COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) AS pv_uv,
    COUNT(CASE WHEN behavior_type = 'fav' THEN 1 END) AS fav_cnt,
    COUNT(CASE WHEN behavior_type = 'cart' THEN 1 END) AS cart_cnt,
    COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt,
    COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS buy_uv,
    ROUND(
        CAST(COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS DOUBLE)
            / COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END),
        4
    ) AS hourly_buy_rate
FROM dwd_user_behavior_clean
WHERE user_id IS NOT NULL
  AND event_hour BETWEEN 0 AND 23
  AND behavior_type IN ('pv', 'fav', 'cart', 'buy')
GROUP BY event_hour
HAVING COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) > 0;

SELECT * FROM lb_time_hour_distribution ORDER BY event_hour;
