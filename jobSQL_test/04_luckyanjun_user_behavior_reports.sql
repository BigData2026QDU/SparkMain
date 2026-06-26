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
    COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS buy_uv,
    ROUND(
        CASE
            WHEN COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) = 0 THEN 0
            ELSE CAST(COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS DOUBLE)
                / COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END)
        END,
        4
    ) AS category_conversion_rate
FROM dwd_user_behavior_clean
WHERE user_id IS NOT NULL
  AND item_id IS NOT NULL
  AND category_id IS NOT NULL
  AND behavior_type IN ('pv', 'fav', 'cart', 'buy')
GROUP BY category_id
HAVING COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) > 0;

DROP TABLE IF EXISTS lb_item_efficiency;
CREATE TABLE lb_item_efficiency AS
SELECT
    item_id,
    category_id,
    COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END) AS pv_cnt,
    COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) AS pv_uv,
    COUNT(CASE WHEN behavior_type = 'fav' THEN 1 END) AS fav_cnt,
    COUNT(CASE WHEN behavior_type = 'cart' THEN 1 END) AS cart_cnt,
    COUNT(CASE WHEN behavior_type IN ('fav', 'cart') THEN 1 END) AS intent_cnt,
    COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt,
    COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS buy_uv,
    ROUND(
        CASE
            WHEN COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) = 0 THEN 0
            ELSE CAST(COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS DOUBLE)
                / COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END)
        END,
        4
    ) AS item_conversion_rate
FROM dwd_user_behavior_clean
WHERE user_id IS NOT NULL
  AND item_id IS NOT NULL
  AND category_id IS NOT NULL
  AND behavior_type IN ('pv', 'fav', 'cart', 'buy')
GROUP BY item_id, category_id
HAVING COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) > 0;

DROP TABLE IF EXISTS lb_category_topn;
CREATE TABLE lb_category_topn AS
SELECT *
FROM (
    SELECT
        ROW_NUMBER() OVER (ORDER BY pv_cnt DESC, buy_cnt DESC, category_id ASC) AS rank_no,
        category_id,
        pv_cnt,
        pv_uv,
        fav_cnt,
        cart_cnt,
        buy_cnt,
        buy_uv,
        category_conversion_rate
    FROM lb_category_efficiency
) ranked
WHERE rank_no <= 20;

DROP TABLE IF EXISTS lb_item_topn;
CREATE TABLE lb_item_topn AS
SELECT *
FROM (
    SELECT
        ROW_NUMBER() OVER (ORDER BY pv_cnt DESC, buy_cnt DESC, item_id ASC) AS rank_no,
        item_id,
        category_id,
        pv_cnt,
        pv_uv,
        intent_cnt,
        buy_cnt,
        buy_uv,
        item_conversion_rate
    FROM lb_item_efficiency
) ranked
WHERE rank_no <= 20;

DROP TABLE IF EXISTS lb_category_conversion_rank;
CREATE TABLE lb_category_conversion_rank AS
SELECT *
FROM (
    SELECT
        ROW_NUMBER() OVER (ORDER BY category_conversion_rate DESC, buy_uv DESC, pv_cnt DESC) AS rank_no,
        category_id,
        pv_cnt,
        pv_uv,
        fav_cnt,
        cart_cnt,
        buy_cnt,
        buy_uv,
        category_conversion_rate
    FROM lb_category_efficiency
    WHERE pv_uv >= 1
) ranked
WHERE rank_no <= 20;

DROP TABLE IF EXISTS lb_category_low_conversion;
CREATE TABLE lb_category_low_conversion AS
SELECT *
FROM (
    SELECT
        ROW_NUMBER() OVER (ORDER BY category_conversion_rate ASC, pv_cnt DESC) AS rank_no,
        category_id,
        pv_cnt,
        pv_uv,
        fav_cnt,
        cart_cnt,
        buy_cnt,
        buy_uv,
        category_conversion_rate
    FROM lb_category_efficiency
    WHERE pv_cnt >= (SELECT AVG(pv_cnt) FROM lb_category_efficiency)
) ranked
WHERE rank_no <= 20;

DROP TABLE IF EXISTS lb_item_long_tail;
CREATE TABLE lb_item_long_tail AS
SELECT
    rank_no,
    item_id,
    category_id,
    pv_cnt,
    pv_uv,
    buy_cnt,
    buy_uv,
    item_conversion_rate,
    cumulative_pv,
    total_pv,
    cumulative_pv_rate,
    CASE
        WHEN cumulative_pv_rate <= 0.8 THEN 'head'
        WHEN cumulative_pv_rate <= 0.95 THEN 'middle'
        ELSE 'long_tail'
    END AS tail_segment
FROM (
    SELECT
        ROW_NUMBER() OVER (ORDER BY pv_cnt DESC, buy_cnt DESC, item_id ASC) AS rank_no,
        item_id,
        category_id,
        pv_cnt,
        pv_uv,
        buy_cnt,
        buy_uv,
        item_conversion_rate,
        SUM(pv_cnt) OVER (ORDER BY pv_cnt DESC, buy_cnt DESC, item_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pv,
        SUM(pv_cnt) OVER () AS total_pv,
        ROUND(
            CAST(SUM(pv_cnt) OVER (ORDER BY pv_cnt DESC, buy_cnt DESC, item_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DOUBLE)
                / SUM(pv_cnt) OVER (),
            4
        ) AS cumulative_pv_rate
    FROM lb_item_efficiency
) ranked;

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
WHERE user_id IS NOT NULL
  AND item_id IS NOT NULL
  AND event_date IS NOT NULL
  AND event_date <> 'event_date'
  AND behavior_type IN ('pv', 'fav', 'cart', 'buy')
GROUP BY user_id;

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
    END AS user_segment,
    CASE WHEN buy_days >= 2 THEN 1 ELSE 0 END AS is_short_repurchase
FROM (
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
        CUME_DIST() OVER (ORDER BY active_days) AS active_days_percentile,
        CUME_DIST() OVER (ORDER BY behavior_cnt) AS behavior_cnt_percentile
    FROM lb_user_features
) ranked;

DROP TABLE IF EXISTS lb_user_segment_summary;
CREATE TABLE lb_user_segment_summary AS
SELECT
    'lifecycle' AS segment_type,
    user_segment,
    COUNT(*) AS user_cnt,
    ROUND(COUNT(*) / CAST(MAX(total_users) AS DOUBLE), 4) AS user_rate,
    ROUND(AVG(active_days), 2) AS avg_active_days,
    ROUND(AVG(behavior_cnt), 2) AS avg_behavior_cnt,
    ROUND(AVG(distinct_item_cnt), 2) AS avg_distinct_item_cnt,
    ROUND(AVG(pv_cnt), 2) AS avg_pv_cnt,
    ROUND(AVG(intent_cnt), 2) AS avg_intent_cnt,
    ROUND(AVG(buy_cnt), 2) AS avg_buy_cnt
FROM (
    SELECT s.*, t.total_users
    FROM (
        SELECT *, 1 AS join_key
        FROM lb_user_segments
    ) s
    JOIN (
        SELECT 1 AS join_key, COUNT(*) AS total_users
        FROM lb_user_segments
    ) t ON s.join_key = t.join_key
) base
GROUP BY user_segment
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
    active_days,
    COUNT(*) AS user_cnt,
    ROUND(COUNT(*) / CAST(MAX(total_users) AS DOUBLE), 4) AS user_rate,
    ROUND(AVG(behavior_cnt), 2) AS avg_behavior_cnt,
    ROUND(AVG(distinct_item_cnt), 2) AS avg_distinct_item_cnt,
    ROUND(AVG(buy_cnt), 2) AS avg_buy_cnt
FROM (
    SELECT s.*, t.total_users
    FROM (
        SELECT *, 1 AS join_key
        FROM lb_user_segments
    ) s
    JOIN (
        SELECT 1 AS join_key, COUNT(*) AS total_users
        FROM lb_user_segments
    ) t ON s.join_key = t.join_key
) base
GROUP BY active_days;

DROP TABLE IF EXISTS lb_user_retention;
CREATE TABLE lb_user_retention AS
SELECT
    f.cohort_date,
    COUNT(DISTINCT f.user_id) AS cohort_users,
    COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 1 THEN f.user_id END) AS day1_retained_users,
    ROUND(COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 1 THEN f.user_id END) / COUNT(DISTINCT f.user_id), 4) AS day1_retention_rate,
    COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 3 THEN f.user_id END) AS day3_retained_users,
    ROUND(COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 3 THEN f.user_id END) / COUNT(DISTINCT f.user_id), 4) AS day3_retention_rate,
    COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 7 THEN f.user_id END) AS day7_retained_users,
    ROUND(COUNT(DISTINCT CASE WHEN datediff(b.event_date, f.cohort_date) = 7 THEN f.user_id END) / COUNT(DISTINCT f.user_id), 4) AS day7_retention_rate
FROM lb_user_features f
LEFT JOIN (
    SELECT DISTINCT user_id, event_date
    FROM dwd_user_behavior_clean
    WHERE user_id IS NOT NULL
      AND event_date IS NOT NULL
      AND event_date <> 'event_date'
) b ON f.user_id = b.user_id
GROUP BY f.cohort_date;

DROP TABLE IF EXISTS lb_user_retention_heatmap;
CREATE TABLE lb_user_retention_heatmap AS
SELECT cohort_date, 1 AS retention_day, cohort_users, day1_retained_users AS retained_users, day1_retention_rate AS retention_rate
FROM lb_user_retention
UNION ALL
SELECT cohort_date, 3 AS retention_day, cohort_users, day3_retained_users AS retained_users, day3_retention_rate AS retention_rate
FROM lb_user_retention
UNION ALL
SELECT cohort_date, 7 AS retention_day, cohort_users, day7_retained_users AS retained_users, day7_retention_rate AS retention_rate
FROM lb_user_retention;

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

SELECT * FROM lb_funnel_overall;
SELECT * FROM lb_user_segment_summary;
