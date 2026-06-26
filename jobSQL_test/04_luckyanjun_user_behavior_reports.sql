-- LuckyAnJun 测试模式: 淘宝用户行为 4 个离线分析报表
SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana_test;

DROP TABLE IF EXISTS lb_funnel_overall;
CREATE TABLE lb_funnel_overall AS
SELECT
    pv_users,
    intent_users,
    buy_users,
    ROUND(intent_users / pv_users, 4) AS pv_to_intent_rate,
    ROUND(buy_users / intent_users, 4) AS intent_to_buy_rate,
    ROUND(buy_users / pv_users, 4) AS pv_to_buy_rate
FROM (
    SELECT
        COUNT(DISTINCT CASE WHEN has_pv = 1 THEN user_id END) AS pv_users,
        COUNT(DISTINCT CASE WHEN has_intent = 1 THEN user_id END) AS intent_users,
        COUNT(DISTINCT CASE WHEN has_buy = 1 THEN user_id END) AS buy_users
    FROM v_user_item_day_flags
) s
WHERE pv_users > 0 AND intent_users > 0;

DROP TABLE IF EXISTS lb_funnel_daily;
CREATE TABLE lb_funnel_daily AS
SELECT
    event_date,
    COUNT(DISTINCT CASE WHEN has_pv = 1 THEN user_id END) AS pv_users,
    COUNT(DISTINCT CASE WHEN has_intent = 1 THEN user_id END) AS intent_users,
    COUNT(DISTINCT CASE WHEN has_buy = 1 THEN user_id END) AS buy_users
FROM v_user_item_day_flags
GROUP BY event_date;

DROP TABLE IF EXISTS lb_time_hourly_behavior;
CREATE TABLE lb_time_hourly_behavior AS
SELECT
    event_date,
    event_hour,
    COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END) AS pv,
    COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) AS pv_uv,
    COUNT(CASE WHEN behavior_type = 'fav' THEN 1 END) AS fav_cnt,
    COUNT(CASE WHEN behavior_type = 'cart' THEN 1 END) AS cart_cnt,
    COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt,
    COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS buy_uv
FROM dwd_user_behavior_clean
GROUP BY event_date, event_hour;

DROP TABLE IF EXISTS lb_time_weekday_hour_heatmap;
CREATE TABLE lb_time_weekday_hour_heatmap AS
SELECT
    weekday,
    event_hour,
    SUM(pv) AS pv,
    SUM(buy_cnt) AS buy_cnt
FROM lb_time_hourly_behavior
GROUP BY weekday, event_hour;

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
