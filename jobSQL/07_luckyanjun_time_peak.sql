-- LuckyAnJun 离线分析2: 时段流量与购买高峰分析
SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana;

DROP TABLE IF EXISTS lb_time_hourly_behavior;
CREATE EXTERNAL TABLE lb_time_hourly_behavior (
    event_date STRING,
    event_hour INT,
    weekday INT,
    pv BIGINT,
    pv_uv BIGINT,
    fav_cnt BIGINT,
    cart_cnt BIGINT,
    buy_cnt BIGINT,
    buy_uv BIGINT,
    hourly_buy_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana/lb_time_hourly_behavior';

INSERT OVERWRITE TABLE lb_time_hourly_behavior
SELECT
    event_date,
    event_hour,
    MAX(weekday) AS weekday,
    COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END) AS pv,
    COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) AS pv_uv,
    COUNT(CASE WHEN behavior_type = 'fav' THEN 1 END) AS fav_cnt,
    COUNT(CASE WHEN behavior_type = 'cart' THEN 1 END) AS cart_cnt,
    COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt,
    COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS buy_uv,
    ROUND(
        CASE
            WHEN COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) = 0 THEN 0
            ELSE CAST(COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS DOUBLE)
                / COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END)
        END,
        4
    ) AS hourly_buy_rate
FROM dwd_user_behavior_clean
WHERE user_id IS NOT NULL
  AND event_date IS NOT NULL
  AND event_date <> 'event_date'
  AND event_hour BETWEEN 0 AND 23
  AND weekday BETWEEN 1 AND 7
  AND behavior_type IN ('pv', 'fav', 'cart', 'buy')
GROUP BY event_date, event_hour
HAVING COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) > 0;

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
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
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
        CASE
            WHEN COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) = 0 THEN 0
            ELSE CAST(COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS DOUBLE)
                / COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END)
        END,
        4
    ) AS hourly_buy_rate
FROM dwd_user_behavior_clean
WHERE user_id IS NOT NULL
  AND event_date IS NOT NULL
  AND event_date <> 'event_date'
  AND event_hour BETWEEN 0 AND 23
  AND behavior_type IN ('pv', 'fav', 'cart', 'buy')
GROUP BY event_hour
HAVING COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) > 0;

DROP TABLE IF EXISTS lb_time_weekday_hour_heatmap;
CREATE EXTERNAL TABLE lb_time_weekday_hour_heatmap (
    weekday INT,
    event_hour INT,
    pv BIGINT,
    pv_uv BIGINT,
    buy_cnt BIGINT,
    buy_uv BIGINT,
    buy_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana/lb_time_weekday_hour_heatmap';

INSERT OVERWRITE TABLE lb_time_weekday_hour_heatmap
SELECT
    weekday,
    event_hour,
    COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END) AS pv,
    COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) AS pv_uv,
    COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt,
    COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS buy_uv,
    ROUND(
        CASE
            WHEN COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) = 0 THEN 0
            ELSE CAST(COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS DOUBLE)
                / COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END)
        END,
        4
    ) AS buy_rate
FROM dwd_user_behavior_clean
WHERE user_id IS NOT NULL
  AND event_date IS NOT NULL
  AND event_date <> 'event_date'
  AND event_hour BETWEEN 0 AND 23
  AND weekday BETWEEN 1 AND 7
  AND behavior_type IN ('pv', 'fav', 'cart', 'buy')
GROUP BY weekday, event_hour
HAVING COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) > 0;

DROP TABLE IF EXISTS lb_time_high_conversion_slots;
CREATE EXTERNAL TABLE lb_time_high_conversion_slots (
    event_date STRING,
    event_hour INT,
    weekday INT,
    pv BIGINT,
    pv_uv BIGINT,
    fav_cnt BIGINT,
    cart_cnt BIGINT,
    buy_cnt BIGINT,
    buy_uv BIGINT,
    hourly_buy_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana/lb_time_high_conversion_slots';

INSERT OVERWRITE TABLE lb_time_high_conversion_slots
SELECT *
FROM lb_time_hourly_behavior
WHERE pv_uv >= 100
ORDER BY hourly_buy_rate DESC, buy_uv DESC, pv DESC
LIMIT 10;

DROP TABLE IF EXISTS lb_time_low_conversion_slots;
CREATE EXTERNAL TABLE lb_time_low_conversion_slots (
    event_date STRING,
    event_hour INT,
    weekday INT,
    pv BIGINT,
    pv_uv BIGINT,
    fav_cnt BIGINT,
    cart_cnt BIGINT,
    buy_cnt BIGINT,
    buy_uv BIGINT,
    hourly_buy_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana/lb_time_low_conversion_slots';

INSERT OVERWRITE TABLE lb_time_low_conversion_slots
SELECT *
FROM lb_time_hourly_behavior
WHERE pv >= (SELECT AVG(pv) FROM lb_time_hourly_behavior)
ORDER BY hourly_buy_rate ASC, pv DESC
LIMIT 10;

SELECT * FROM lb_time_hourly_behavior ORDER BY event_date, event_hour LIMIT 24;
SELECT * FROM lb_time_hour_distribution ORDER BY event_hour;
SELECT * FROM lb_time_weekday_hour_heatmap ORDER BY weekday, event_hour LIMIT 50;
SELECT * FROM lb_time_high_conversion_slots;
SELECT * FROM lb_time_low_conversion_slots;
