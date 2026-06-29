-- LuckyAnJun 离线分析 #15：同一用户、同一商品、严格时间顺序漏斗。
SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana;

DROP TABLE IF EXISTS lb_funnel_item_path_stage;
CREATE EXTERNAL TABLE lb_funnel_item_path_stage (
    stage_order INT,
    stage_name STRING,
    pair_cnt BIGINT,
    conversion_rate DOUBLE
)
STORED AS PARQUET
LOCATION '/user/hive/bigdata_ana/lb_funnel_item_path_stage';

INSERT OVERWRITE TABLE lb_funnel_item_path_stage
WITH item_events AS (
    SELECT
        CAST(user_id AS BIGINT) AS user_id,
        CAST(item_id AS BIGINT) AS item_id,
        LOWER(behavior_type) AS behavior_type,
        CAST(`timestamp` AS BIGINT) AS event_ts
    FROM dwd_user_behavior_clean
    WHERE user_id IS NOT NULL
      AND item_id IS NOT NULL
      AND `timestamp` IS NOT NULL
      AND LOWER(behavior_type) IN ('pv', 'fav', 'cart', 'buy')
),
first_pv AS (
    SELECT
        user_id,
        item_id,
        MIN(CASE WHEN behavior_type = 'pv' THEN event_ts END) AS first_pv_ts
    FROM item_events
    GROUP BY user_id, item_id
    HAVING MIN(CASE WHEN behavior_type = 'pv' THEN event_ts END) IS NOT NULL
),
first_intent_after_pv AS (
    SELECT
        e.user_id,
        e.item_id,
        MIN(e.event_ts) AS first_intent_after_pv_ts
    FROM item_events e
    JOIN first_pv p
      ON e.user_id = p.user_id
     AND e.item_id = p.item_id
    WHERE e.behavior_type IN ('fav', 'cart')
      AND e.event_ts >= p.first_pv_ts
    GROUP BY e.user_id, e.item_id
),
buy_after_intent AS (
    SELECT
        e.user_id,
        e.item_id
    FROM item_events e
    JOIN first_intent_after_pv i
      ON e.user_id = i.user_id
     AND e.item_id = i.item_id
    WHERE e.behavior_type = 'buy'
      AND e.event_ts >= i.first_intent_after_pv_ts
    GROUP BY e.user_id, e.item_id
),
path_counts AS (
    SELECT
        (SELECT COUNT(*) FROM first_pv) AS viewed_pairs,
        (SELECT COUNT(*) FROM first_intent_after_pv) AS intent_pairs,
        (SELECT COUNT(*) FROM buy_after_intent) AS bought_pairs
)
SELECT
    stage_order,
    stage_name,
    pair_cnt,
    conversion_rate
FROM path_counts
LATERAL VIEW STACK(
    3,
    1, '浏览商品', viewed_pairs, CAST(1.0 AS DOUBLE),
    2, '浏览后收藏/加购同商品', intent_pairs,
       ROUND(CASE WHEN viewed_pairs = 0 THEN 0
             ELSE CAST(intent_pairs AS DOUBLE) / viewed_pairs END, 6),
    3, '意向后购买同商品', bought_pairs,
       ROUND(CASE WHEN viewed_pairs = 0 THEN 0
             ELSE CAST(bought_pairs AS DOUBLE) / viewed_pairs END, 6)
) stages AS stage_order, stage_name, pair_cnt, conversion_rate;

SELECT * FROM lb_funnel_item_path_stage ORDER BY stage_order;
