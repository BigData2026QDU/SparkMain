-- LuckyAnJun offline analysis #15: user behavior funnel conversion and loss.
-- Intent is defined as fav or cart. The funnel does not assume a strict
-- pv -> fav -> cart -> buy sequence.

SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana;

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
LOCATION '/user/hive/bigdata_ana/lb_funnel_overall';

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
LOCATION '/user/hive/bigdata_ana/lb_funnel_daily';

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

SELECT * FROM lb_funnel_overall;
SELECT * FROM lb_funnel_daily ORDER BY event_date LIMIT 20;
