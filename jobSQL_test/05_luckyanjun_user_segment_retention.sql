-- LuckyAnJun test analysis 4: user segmentation, retention and short-term repurchase.
SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana_test;

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
    COUNT(CASE WHEN behavior_type = 'fav' THEN 1 END) AS fav_cnt,
    COUNT(CASE WHEN behavior_type = 'cart' THEN 1 END) AS cart_cnt,
    COUNT(CASE WHEN behavior_type IN ('fav', 'cart') THEN 1 END) AS intent_cnt,
    COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt,
    COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN event_date END) AS buy_days
FROM dwd_user_behavior_clean
GROUP BY user_id;

DROP TABLE IF EXISTS lb_user_activity_ranked;
CREATE TABLE lb_user_activity_ranked AS
SELECT
    user_id,
    cohort_date,
    last_event_date,
    behavior_cnt,
    active_days,
    distinct_item_cnt,
    pv_cnt,
    fav_cnt,
    cart_cnt,
    intent_cnt,
    buy_cnt,
    buy_days,
    CUME_DIST() OVER (ORDER BY active_days) AS active_days_percentile,
    CUME_DIST() OVER (ORDER BY behavior_cnt) AS behavior_cnt_percentile
FROM lb_user_features;

DROP TABLE IF EXISTS lb_user_segments;
CREATE TABLE lb_user_segments AS
SELECT
    user_id,
    cohort_date,
    last_event_date,
    active_days,
    behavior_cnt,
    distinct_item_cnt,
    pv_cnt,
    fav_cnt,
    cart_cnt,
    intent_cnt,
    buy_cnt,
    buy_days,
    CASE
        WHEN active_days_percentile >= 0.8 AND behavior_cnt_percentile >= 0.8 THEN 1
        ELSE 0
    END AS is_high_active,
    CASE
        WHEN pv_cnt > 0 AND intent_cnt = 0 AND buy_cnt = 0 THEN 'browse_only'
        WHEN intent_cnt > 0 AND buy_cnt = 0 THEN 'intent_only'
        WHEN buy_days = 1 THEN 'first_buy'
        WHEN buy_days >= 2 THEN 'short_repurchase'
        ELSE 'other'
    END AS lifecycle_segment,
    CASE
        WHEN buy_days >= 2 THEN 1
        ELSE 0
    END AS is_short_repurchase
FROM lb_user_activity_ranked;

DROP TABLE IF EXISTS lb_user_total;
CREATE TABLE lb_user_total AS
SELECT
    1 AS join_key,
    COUNT(*) AS total_users
FROM lb_user_segments;

DROP TABLE IF EXISTS lb_user_segment_summary;
CREATE TABLE lb_user_segment_summary AS
SELECT
    'lifecycle' AS segment_type,
    s.lifecycle_segment AS user_segment,
    COUNT(*) AS user_cnt,
    ROUND(COUNT(*) / CAST(t.total_users AS DOUBLE), 4) AS user_rate,
    ROUND(AVG(s.active_days), 2) AS avg_active_days,
    ROUND(AVG(s.behavior_cnt), 2) AS avg_behavior_cnt,
    ROUND(AVG(s.distinct_item_cnt), 2) AS avg_distinct_item_cnt,
    ROUND(AVG(s.pv_cnt), 2) AS avg_pv_cnt,
    ROUND(AVG(s.intent_cnt), 2) AS avg_intent_cnt,
    ROUND(AVG(s.buy_cnt), 2) AS avg_buy_cnt
FROM (
    SELECT *, 1 AS join_key
    FROM lb_user_segments
) s
JOIN lb_user_total t ON s.join_key = t.join_key
GROUP BY s.lifecycle_segment, t.total_users
UNION ALL
SELECT
    'activity' AS segment_type,
    'high_active' AS user_segment,
    COUNT(CASE WHEN is_high_active = 1 THEN 1 END) AS user_cnt,
    ROUND(COUNT(CASE WHEN is_high_active = 1 THEN 1 END) / CAST(COUNT(*) AS DOUBLE), 4) AS user_rate,
    ROUND(AVG(CASE WHEN is_high_active = 1 THEN active_days END), 2) AS avg_active_days,
    ROUND(AVG(CASE WHEN is_high_active = 1 THEN behavior_cnt END), 2) AS avg_behavior_cnt,
    ROUND(AVG(CASE WHEN is_high_active = 1 THEN distinct_item_cnt END), 2) AS avg_distinct_item_cnt,
    ROUND(AVG(CASE WHEN is_high_active = 1 THEN pv_cnt END), 2) AS avg_pv_cnt,
    ROUND(AVG(CASE WHEN is_high_active = 1 THEN intent_cnt END), 2) AS avg_intent_cnt,
    ROUND(AVG(CASE WHEN is_high_active = 1 THEN buy_cnt END), 2) AS avg_buy_cnt
FROM lb_user_segments;

DROP TABLE IF EXISTS lb_user_active_day_distribution;
CREATE TABLE lb_user_active_day_distribution AS
SELECT
    s.active_days,
    COUNT(*) AS user_cnt,
    ROUND(COUNT(*) / CAST(t.total_users AS DOUBLE), 4) AS user_rate,
    ROUND(AVG(s.behavior_cnt), 2) AS avg_behavior_cnt,
    ROUND(AVG(s.distinct_item_cnt), 2) AS avg_distinct_item_cnt,
    ROUND(AVG(s.buy_cnt), 2) AS avg_buy_cnt
FROM (
    SELECT *, 1 AS join_key
    FROM lb_user_segments
) s
JOIN lb_user_total t ON s.join_key = t.join_key
GROUP BY s.active_days, t.total_users;

DROP TABLE IF EXISTS lb_user_retention;
CREATE TABLE lb_user_retention AS
SELECT
    f.cohort_date,
    COUNT(DISTINCT f.user_id) AS cohort_users,
    COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 1 THEN f.user_id END) AS day1_retained_users,
    ROUND(COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 1 THEN f.user_id END) / CAST(COUNT(DISTINCT f.user_id) AS DOUBLE), 4) AS day1_retention_rate,
    COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 3 THEN f.user_id END) AS day3_retained_users,
    ROUND(COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 3 THEN f.user_id END) / CAST(COUNT(DISTINCT f.user_id) AS DOUBLE), 4) AS day3_retention_rate,
    COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 7 THEN f.user_id END) AS day7_retained_users,
    ROUND(COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 7 THEN f.user_id END) / CAST(COUNT(DISTINCT f.user_id) AS DOUBLE), 4) AS day7_retention_rate
FROM lb_user_features f
LEFT JOIN dwd_user_behavior_clean b ON f.user_id = b.user_id
GROUP BY f.cohort_date;

DROP TABLE IF EXISTS lb_repurchase_behavior_depth;
CREATE TABLE lb_repurchase_behavior_depth AS
SELECT
    CASE
        WHEN is_short_repurchase = 1 THEN 'short_repurchase'
        ELSE 'non_repurchase'
    END AS repurchase_group,
    COUNT(*) AS user_cnt,
    ROUND(AVG(active_days), 2) AS avg_active_days,
    ROUND(AVG(behavior_cnt), 2) AS avg_behavior_cnt,
    ROUND(AVG(distinct_item_cnt), 2) AS avg_distinct_item_cnt,
    ROUND(AVG(pv_cnt), 2) AS avg_pv_cnt,
    ROUND(AVG(fav_cnt), 2) AS avg_fav_cnt,
    ROUND(AVG(cart_cnt), 2) AS avg_cart_cnt,
    ROUND(AVG(intent_cnt), 2) AS avg_intent_cnt,
    ROUND(AVG(buy_cnt), 2) AS avg_buy_cnt,
    ROUND(AVG(buy_days), 2) AS avg_buy_days
FROM lb_user_segments
GROUP BY CASE
    WHEN is_short_repurchase = 1 THEN 'short_repurchase'
    ELSE 'non_repurchase'
END;

SELECT * FROM lb_user_segment_summary ORDER BY segment_type, user_cnt DESC;
SELECT * FROM lb_user_active_day_distribution ORDER BY active_days;
SELECT * FROM lb_user_retention ORDER BY cohort_date LIMIT 20;
SELECT * FROM lb_repurchase_behavior_depth ORDER BY repurchase_group;
