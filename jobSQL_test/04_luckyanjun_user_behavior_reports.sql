-- LuckyAnJun test mode: Taobao UserBehavior offline analysis reports.

SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana_test;

DROP TABLE IF EXISTS lb_funnel_overall;
CREATE EXTERNAL TABLE lb_funnel_overall (
    pv_users BIGINT,
    intent_users BIGINT,
    buy_users BIGINT,
    pv_to_intent_users BIGINT,
    intent_to_buy_users BIGINT,
    pv_to_buy_users BIGINT,
    pv_to_intent_rate DOUBLE,
    intent_to_buy_rate DOUBLE,
    pv_to_buy_rate DOUBLE,
    pv_loss_users BIGINT,
    intent_loss_users BIGINT,
    pv_loss_rate DOUBLE,
    intent_loss_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana_test/lb_funnel_overall';

INSERT OVERWRITE TABLE lb_funnel_overall
SELECT
    pv_users,
    intent_users,
    buy_users,
    pv_to_intent_users,
    intent_to_buy_users,
    pv_to_buy_users,
    ROUND(CASE WHEN pv_users = 0 THEN 0 ELSE CAST(pv_to_intent_users AS DOUBLE) / pv_users END, 4) AS pv_to_intent_rate,
    ROUND(CASE WHEN intent_users = 0 THEN 0 ELSE CAST(intent_to_buy_users AS DOUBLE) / intent_users END, 4) AS intent_to_buy_rate,
    ROUND(CASE WHEN pv_users = 0 THEN 0 ELSE CAST(pv_to_buy_users AS DOUBLE) / pv_users END, 4) AS pv_to_buy_rate,
    pv_loss_users,
    intent_loss_users,
    ROUND(CASE WHEN pv_users = 0 THEN 0 ELSE CAST(pv_loss_users AS DOUBLE) / pv_users END, 4) AS pv_loss_rate,
    ROUND(CASE WHEN intent_users = 0 THEN 0 ELSE CAST(intent_loss_users AS DOUBLE) / intent_users END, 4) AS intent_loss_rate
FROM (
    SELECT
        COUNT(CASE WHEN has_pv = 1 THEN 1 END) AS pv_users,
        COUNT(CASE WHEN has_intent = 1 THEN 1 END) AS intent_users,
        COUNT(CASE WHEN has_buy = 1 THEN 1 END) AS buy_users,
        COUNT(CASE WHEN has_pv = 1 AND has_intent = 1 THEN 1 END) AS pv_to_intent_users,
        COUNT(CASE WHEN has_intent = 1 AND has_buy = 1 THEN 1 END) AS intent_to_buy_users,
        COUNT(CASE WHEN has_pv = 1 AND has_buy = 1 THEN 1 END) AS pv_to_buy_users,
        COUNT(CASE WHEN has_pv = 1 AND has_intent = 0 THEN 1 END) AS pv_loss_users,
        COUNT(CASE WHEN has_intent = 1 AND has_buy = 0 THEN 1 END) AS intent_loss_users
    FROM (
        SELECT
            user_id,
            MAX(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS has_pv,
            MAX(CASE WHEN behavior_type IN ('fav', 'cart') THEN 1 ELSE 0 END) AS has_intent,
            MAX(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS has_buy
        FROM dwd_user_behavior_clean
        GROUP BY user_id
    ) user_stage_flags
) stage_counts;

DROP TABLE IF EXISTS lb_funnel_daily;
CREATE EXTERNAL TABLE lb_funnel_daily (
    event_date STRING,
    pv_users BIGINT,
    intent_users BIGINT,
    buy_users BIGINT,
    pv_to_intent_users BIGINT,
    intent_to_buy_users BIGINT,
    pv_to_buy_users BIGINT,
    pv_to_intent_rate DOUBLE,
    intent_to_buy_rate DOUBLE,
    pv_to_buy_rate DOUBLE,
    pv_loss_users BIGINT,
    intent_loss_users BIGINT,
    pv_loss_rate DOUBLE,
    intent_loss_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana_test/lb_funnel_daily';

INSERT OVERWRITE TABLE lb_funnel_daily
SELECT
    event_date,
    pv_users,
    intent_users,
    buy_users,
    pv_to_intent_users,
    intent_to_buy_users,
    pv_to_buy_users,
    ROUND(CASE WHEN pv_users = 0 THEN 0 ELSE CAST(pv_to_intent_users AS DOUBLE) / pv_users END, 4) AS pv_to_intent_rate,
    ROUND(CASE WHEN intent_users = 0 THEN 0 ELSE CAST(intent_to_buy_users AS DOUBLE) / intent_users END, 4) AS intent_to_buy_rate,
    ROUND(CASE WHEN pv_users = 0 THEN 0 ELSE CAST(pv_to_buy_users AS DOUBLE) / pv_users END, 4) AS pv_to_buy_rate,
    pv_loss_users,
    intent_loss_users,
    ROUND(CASE WHEN pv_users = 0 THEN 0 ELSE CAST(pv_loss_users AS DOUBLE) / pv_users END, 4) AS pv_loss_rate,
    ROUND(CASE WHEN intent_users = 0 THEN 0 ELSE CAST(intent_loss_users AS DOUBLE) / intent_users END, 4) AS intent_loss_rate
FROM (
    SELECT
        event_date,
        COUNT(CASE WHEN has_pv = 1 THEN 1 END) AS pv_users,
        COUNT(CASE WHEN has_intent = 1 THEN 1 END) AS intent_users,
        COUNT(CASE WHEN has_buy = 1 THEN 1 END) AS buy_users,
        COUNT(CASE WHEN has_pv = 1 AND has_intent = 1 THEN 1 END) AS pv_to_intent_users,
        COUNT(CASE WHEN has_intent = 1 AND has_buy = 1 THEN 1 END) AS intent_to_buy_users,
        COUNT(CASE WHEN has_pv = 1 AND has_buy = 1 THEN 1 END) AS pv_to_buy_users,
        COUNT(CASE WHEN has_pv = 1 AND has_intent = 0 THEN 1 END) AS pv_loss_users,
        COUNT(CASE WHEN has_intent = 1 AND has_buy = 0 THEN 1 END) AS intent_loss_users
    FROM (
        SELECT
            event_date,
            user_id,
            MAX(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS has_pv,
            MAX(CASE WHEN behavior_type IN ('fav', 'cart') THEN 1 ELSE 0 END) AS has_intent,
            MAX(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS has_buy
        FROM dwd_user_behavior_clean
        GROUP BY event_date, user_id
    ) user_day_stage_flags
    GROUP BY event_date
) stage_counts;

DROP TABLE IF EXISTS lb_time_hourly_behavior;
CREATE TABLE lb_time_hourly_behavior AS
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
CREATE TABLE lb_time_hour_distribution AS
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
CREATE TABLE lb_time_weekday_hour_heatmap AS
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
CREATE TABLE lb_time_high_conversion_slots AS
SELECT *
FROM lb_time_hourly_behavior
WHERE pv_uv >= 1
ORDER BY hourly_buy_rate DESC, buy_uv DESC, pv DESC
LIMIT 10;

DROP TABLE IF EXISTS lb_time_low_conversion_slots;
CREATE TABLE lb_time_low_conversion_slots AS
SELECT *
FROM lb_time_hourly_behavior
WHERE pv >= (SELECT AVG(pv) FROM lb_time_hourly_behavior)
ORDER BY hourly_buy_rate ASC, pv DESC
LIMIT 10;

DROP TABLE IF EXISTS lb_category_efficiency;
CREATE TABLE lb_category_efficiency AS
SELECT
    category_id,
    COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END) AS pv_cnt,
    COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) AS pv_uv,
    COUNT(CASE WHEN behavior_type = 'fav' THEN 1 END) AS fav_cnt,
    COUNT(CASE WHEN behavior_type = 'cart' THEN 1 END) AS cart_cnt,
    COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt,
    COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS buy_uv
FROM dwd_user_behavior_clean
GROUP BY category_id;

DROP TABLE IF EXISTS lb_item_efficiency;
CREATE TABLE lb_item_efficiency AS
SELECT
    item_id,
    category_id,
    COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END) AS pv_cnt,
    COUNT(CASE WHEN behavior_type IN ('fav', 'cart') THEN 1 END) AS intent_cnt,
    COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt
FROM dwd_user_behavior_clean
GROUP BY item_id, category_id;

DROP TABLE IF EXISTS lb_user_features;
CREATE TABLE lb_user_features AS
SELECT
    user_id,
    MIN(event_date) AS cohort_date,
    COUNT(*) AS behavior_cnt,
    COUNT(DISTINCT event_date) AS active_days,
    COUNT(CASE WHEN behavior_type IN ('fav', 'cart') THEN 1 END) AS intent_cnt,
    COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt,
    COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN event_date END) AS buy_days
FROM dwd_user_behavior_clean
GROUP BY user_id;

DROP TABLE IF EXISTS lb_user_segments;
CREATE TABLE lb_user_segments AS
SELECT
    user_id,
    cohort_date,
    active_days,
    behavior_cnt,
    CASE
        WHEN intent_cnt = 0 AND buy_cnt = 0 THEN 'browse_only'
        WHEN intent_cnt > 0 AND buy_cnt = 0 THEN 'intent_only'
        WHEN buy_days = 1 THEN 'first_buy'
        WHEN buy_days >= 2 THEN 'short_repurchase'
        ELSE 'other'
    END AS user_segment
FROM lb_user_features;

DROP TABLE IF EXISTS lb_user_segment_summary;
CREATE TABLE lb_user_segment_summary AS
SELECT
    user_segment,
    COUNT(*) AS user_cnt,
    ROUND(AVG(active_days), 2) AS avg_active_days,
    ROUND(AVG(behavior_cnt), 2) AS avg_behavior_cnt
FROM lb_user_segments
GROUP BY user_segment;

DROP TABLE IF EXISTS lb_user_retention;
CREATE TABLE lb_user_retention AS
SELECT
    f.cohort_date,
    COUNT(DISTINCT f.user_id) AS cohort_users,
    ROUND(COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 1 THEN f.user_id END) / COUNT(DISTINCT f.user_id), 4) AS day1_retention_rate,
    ROUND(COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 3 THEN f.user_id END) / COUNT(DISTINCT f.user_id), 4) AS day3_retention_rate
FROM lb_user_features f
LEFT JOIN dwd_user_behavior_clean b ON f.user_id = b.user_id
GROUP BY f.cohort_date;

SELECT * FROM lb_funnel_overall;
SELECT * FROM lb_user_segment_summary;
