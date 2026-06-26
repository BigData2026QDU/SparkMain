-- LuckyAnJun 离线分析2: 时段流量与购买高峰分析
SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana;

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
    COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS buy_uv,
    ROUND(
        COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) /
        COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END),
        4
    ) AS hourly_buy_rate
FROM dwd_user_behavior_clean
GROUP BY event_date, event_hour
HAVING pv_uv > 0;

DROP TABLE IF EXISTS lb_time_weekday_hour_heatmap;
CREATE TABLE lb_time_weekday_hour_heatmap AS
SELECT
    weekday,
    event_hour,
    SUM(pv) AS pv,
    SUM(buy_cnt) AS buy_cnt,
    ROUND(SUM(buy_uv) / SUM(pv_uv), 4) AS buy_rate
FROM lb_time_hourly_behavior
GROUP BY weekday, event_hour;

DROP TABLE IF EXISTS lb_time_low_conversion_slots;
CREATE TABLE lb_time_low_conversion_slots AS
SELECT *
FROM lb_time_hourly_behavior
WHERE pv >= 1
ORDER BY hourly_buy_rate ASC, pv DESC
LIMIT 10;

SELECT * FROM lb_time_hourly_behavior ORDER BY event_date, event_hour LIMIT 24;
SELECT * FROM lb_time_weekday_hour_heatmap ORDER BY weekday, event_hour LIMIT 50;
