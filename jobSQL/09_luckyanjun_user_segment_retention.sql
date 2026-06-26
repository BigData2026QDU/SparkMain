-- LuckyAnJun 离线分析4: 用户分层、留存与短期复购分析
SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana;

DROP TABLE IF EXISTS lb_user_features;
CREATE TABLE lb_user_features AS
SELECT
    user_id,
    MIN(event_date) AS cohort_date,
    MAX(event_date) AS last_event_date,
    COUNT(*) AS behavior_cnt,
    COUNT(DISTINCT event_date) AS active_days,
    COUNT(DISTINCT item_id) AS distinct_item_cnt,
    COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END) AS pv_cnt,
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
    distinct_item_cnt,
    buy_cnt,
    buy_days,
    CASE
        WHEN pv_cnt > 0 AND intent_cnt = 0 AND buy_cnt = 0 THEN 'browse_only'
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
    ROUND(AVG(behavior_cnt), 2) AS avg_behavior_cnt,
    ROUND(AVG(distinct_item_cnt), 2) AS avg_distinct_item_cnt,
    ROUND(AVG(buy_cnt), 2) AS avg_buy_cnt
FROM lb_user_segments
GROUP BY user_segment;

DROP TABLE IF EXISTS lb_user_retention;
CREATE TABLE lb_user_retention AS
SELECT
    f.cohort_date,
    COUNT(DISTINCT f.user_id) AS cohort_users,
    ROUND(COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 1 THEN f.user_id END) / COUNT(DISTINCT f.user_id), 4) AS day1_retention_rate,
    ROUND(COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 3 THEN f.user_id END) / COUNT(DISTINCT f.user_id), 4) AS day3_retention_rate,
    ROUND(COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 7 THEN f.user_id END) / COUNT(DISTINCT f.user_id), 4) AS day7_retention_rate
FROM lb_user_features f
LEFT JOIN dwd_user_behavior_clean b ON f.user_id = b.user_id
GROUP BY f.cohort_date;

SELECT * FROM lb_user_segment_summary ORDER BY user_cnt DESC;
SELECT * FROM lb_user_retention ORDER BY cohort_date LIMIT 20;
