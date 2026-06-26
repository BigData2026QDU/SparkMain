-- LuckyAnJun 离线分析1: 用户行为漏斗与转化流失分析
SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana;

DROP TABLE IF EXISTS lb_funnel_overall;
CREATE TABLE lb_funnel_overall AS
SELECT
    pv_users,
    intent_users,
    buy_users,
    ROUND(intent_users / pv_users, 4) AS pv_to_intent_rate,
    ROUND(buy_users / intent_users, 4) AS intent_to_buy_rate,
    ROUND(buy_users / pv_users, 4) AS pv_to_buy_rate,
    pv_users - intent_users AS pv_loss_users,
    intent_users - buy_users AS intent_loss_users
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
    pv_users,
    intent_users,
    buy_users,
    ROUND(intent_users / pv_users, 4) AS pv_to_intent_rate,
    ROUND(buy_users / intent_users, 4) AS intent_to_buy_rate,
    ROUND(buy_users / pv_users, 4) AS pv_to_buy_rate
FROM (
    SELECT
        event_date,
        COUNT(DISTINCT CASE WHEN has_pv = 1 THEN user_id END) AS pv_users,
        COUNT(DISTINCT CASE WHEN has_intent = 1 THEN user_id END) AS intent_users,
        COUNT(DISTINCT CASE WHEN has_buy = 1 THEN user_id END) AS buy_users
    FROM v_user_item_day_flags
    GROUP BY event_date
) s
WHERE pv_users > 0 AND intent_users > 0;

SELECT * FROM lb_funnel_overall;
SELECT * FROM lb_funnel_daily ORDER BY event_date LIMIT 20;
