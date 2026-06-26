-- LuckyAnJun 离线分析3: 类目热度与商品转化效率分析
SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana;

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
        COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) /
        COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END),
        4
    ) AS category_conversion_rate
FROM dwd_user_behavior_clean
GROUP BY category_id
HAVING pv_uv > 0;

DROP TABLE IF EXISTS lb_item_efficiency;
CREATE TABLE lb_item_efficiency AS
SELECT
    item_id,
    category_id,
    COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END) AS pv_cnt,
    COUNT(CASE WHEN behavior_type IN ('fav', 'cart') THEN 1 END) AS intent_cnt,
    COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt,
    COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS buy_uv,
    ROUND(
        COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) /
        COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END),
        4
    ) AS item_conversion_rate
FROM dwd_user_behavior_clean
GROUP BY item_id, category_id
HAVING pv_cnt > 0;

DROP TABLE IF EXISTS lb_item_long_tail;
CREATE TABLE lb_item_long_tail AS
SELECT
    item_id,
    category_id,
    pv_cnt,
    buy_cnt,
    SUM(pv_cnt) OVER (ORDER BY pv_cnt DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pv,
    SUM(pv_cnt) OVER () AS total_pv,
    ROUND(SUM(pv_cnt) OVER (ORDER BY pv_cnt DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(pv_cnt) OVER (), 4) AS cumulative_pv_rate
FROM lb_item_efficiency;

SELECT * FROM lb_category_efficiency ORDER BY pv_cnt DESC LIMIT 20;
SELECT * FROM lb_item_efficiency ORDER BY buy_cnt DESC, pv_cnt DESC LIMIT 20;
