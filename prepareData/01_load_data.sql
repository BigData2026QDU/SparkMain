-- 数据准备：加载数据到 Hive 表
-- 使用 Hive on Spark 执行

SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana;

-- 验证数据加载
SELECT 'movies' AS table_name, COUNT(*) AS row_count FROM movies
UNION ALL
SELECT 'ratings' AS table_name, COUNT(*) AS row_count FROM ratings
UNION ALL
SELECT 'tags' AS table_name, COUNT(*) AS row_count FROM tags
UNION ALL
SELECT 'links' AS table_name, COUNT(*) AS row_count FROM links;

SELECT 'user_behavior' AS table_name, COUNT(*) AS row_count FROM user_behavior;

-- 创建临时视图用于快速查询
CREATE OR REPLACE VIEW v_movies_ratings AS
SELECT
    m.movieId,
    m.title,
    m.genres,
    r.userId,
    r.rating,
    FROM_UNIXTIME(r.`timestamp`) AS rating_time
FROM movies m
JOIN ratings r ON m.movieId = r.movieId;

DROP TABLE IF EXISTS dwd_user_behavior_clean;
CREATE TABLE dwd_user_behavior_clean AS
SELECT
    user_id,
    item_id,
    category_id,
    behavior_type,
    `timestamp`,
    CAST(event_time AS TIMESTAMP) AS event_time,
    event_date,
    event_hour,
    weekday
FROM user_behavior
WHERE behavior_type IN ('pv', 'buy', 'cart', 'fav')
  AND user_id IS NOT NULL
  AND item_id IS NOT NULL
  AND category_id IS NOT NULL
  AND `timestamp` > 0;

CREATE OR REPLACE VIEW v_user_item_day_flags AS
SELECT
    user_id,
    item_id,
    event_date,
    MAX(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS has_pv,
    MAX(CASE WHEN behavior_type IN ('fav', 'cart') THEN 1 ELSE 0 END) AS has_intent,
    MAX(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS has_buy
FROM dwd_user_behavior_clean
GROUP BY user_id, item_id, event_date;
