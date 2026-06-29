-- CI smoke fixtures for the four final LuckyAnJun offline reports.
SET hive.execution.engine=spark;
SET spark.master=local[*];
USE bigdata_ana_test;

DROP TABLE IF EXISTS lb_funnel_item_path_stage;
CREATE TABLE lb_funnel_item_path_stage AS
WITH item_events AS (
    SELECT
        CAST(user_id AS BIGINT) AS user_id,
        CAST(item_id AS BIGINT) AS item_id,
        LOWER(behavior_type) AS behavior_type,
        CAST(`timestamp` AS BIGINT) AS event_ts
    FROM dwd_user_behavior_clean
),
first_pv AS (
    SELECT user_id, item_id,
           MIN(CASE WHEN behavior_type = 'pv' THEN event_ts END) AS first_pv_ts
    FROM item_events
    GROUP BY user_id, item_id
),
first_intent AS (
    SELECT e.user_id, e.item_id, MIN(e.event_ts) AS first_intent_ts
    FROM item_events e
    JOIN first_pv p ON e.user_id = p.user_id AND e.item_id = p.item_id
    WHERE p.first_pv_ts IS NOT NULL
      AND e.behavior_type IN ('fav', 'cart')
      AND e.event_ts >= p.first_pv_ts
    GROUP BY e.user_id, e.item_id
),
bought AS (
    SELECT e.user_id, e.item_id
    FROM item_events e
    JOIN first_intent i ON e.user_id = i.user_id AND e.item_id = i.item_id
    WHERE e.behavior_type = 'buy' AND e.event_ts >= i.first_intent_ts
    GROUP BY e.user_id, e.item_id
),
counts AS (
    SELECT
        (SELECT COUNT(*) FROM first_pv WHERE first_pv_ts IS NOT NULL) AS viewed_pairs,
        (SELECT COUNT(*) FROM first_intent) AS intent_pairs,
        (SELECT COUNT(*) FROM bought) AS bought_pairs
)
SELECT
    stage_order,
    stage_name,
    pair_cnt,
    conversion_rate
FROM counts
LATERAL VIEW STACK(
    3,
    1, '浏览商品', viewed_pairs, CAST(1.0 AS DOUBLE),
    2, '浏览后收藏/加购同商品', intent_pairs,
       CASE WHEN viewed_pairs = 0 THEN 0.0 ELSE intent_pairs / CAST(viewed_pairs AS DOUBLE) END,
    3, '意向后购买同商品', bought_pairs,
       CASE WHEN viewed_pairs = 0 THEN 0.0 ELSE bought_pairs / CAST(viewed_pairs AS DOUBLE) END
) stages AS stage_order, stage_name, pair_cnt, conversion_rate;

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
        COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END)
            / CAST(COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) AS DOUBLE),
        4
    ) AS hourly_buy_rate
FROM dwd_user_behavior_clean
WHERE event_hour BETWEEN 0 AND 23
GROUP BY event_hour
HAVING COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) > 0;

DROP TABLE IF EXISTS lb_category_topn;
CREATE TABLE lb_category_topn AS
SELECT
    CAST(rank_no AS INT) AS rank_no,
    category_id,
    pv_cnt,
    pv_uv,
    fav_cnt,
    cart_cnt,
    buy_cnt,
    buy_uv,
    ROUND(buy_uv / CAST(pv_uv AS DOUBLE), 4) AS category_conversion_rate
FROM (
    SELECT
        ROW_NUMBER() OVER (ORDER BY pv_cnt DESC, buy_cnt DESC, category_id ASC) AS rank_no,
        *
    FROM (
        SELECT
            category_id,
            COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END) AS pv_cnt,
            COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) AS pv_uv,
            COUNT(CASE WHEN behavior_type = 'fav' THEN 1 END) AS fav_cnt,
            COUNT(CASE WHEN behavior_type = 'cart' THEN 1 END) AS cart_cnt,
            COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt,
            COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS buy_uv
        FROM dwd_user_behavior_clean
        GROUP BY category_id
        HAVING COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) > 0
    ) category_stats
) ranked
WHERE rank_no <= 20;

DROP TABLE IF EXISTS lb_user_active_day_distribution;
CREATE TABLE lb_user_active_day_distribution AS
WITH user_activity AS (
    SELECT
        user_id,
        COUNT(DISTINCT event_date) AS active_days,
        COUNT(*) AS behavior_cnt,
        COUNT(DISTINCT item_id) AS distinct_item_cnt,
        COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt
    FROM dwd_user_behavior_clean
    GROUP BY user_id
),
total_users AS (
    SELECT COUNT(*) AS user_cnt FROM user_activity
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
