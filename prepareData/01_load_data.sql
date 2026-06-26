-- 数据准备：验证外部表并创建 LuckyAnJun UserBehavior DWD 表
-- 使用 Hive on Spark 执行；如集群 Hive Spark 引擎缺少 Scala 依赖，可临时改为 mr 执行。

SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana;

SELECT 'movies' AS table_name, COUNT(*) AS row_count FROM movies
UNION ALL
SELECT 'ratings' AS table_name, COUNT(*) AS row_count FROM ratings
UNION ALL
SELECT 'tags' AS table_name, COUNT(*) AS row_count FROM tags
UNION ALL
SELECT 'links' AS table_name, COUNT(*) AS row_count FROM links;

SELECT 'user_behavior' AS table_name, COUNT(*) AS row_count FROM user_behavior;

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

DROP VIEW IF EXISTS v_user_item_day_flags;
DROP TABLE IF EXISTS dwd_user_behavior_clean;
CREATE EXTERNAL TABLE dwd_user_behavior_clean (
    user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior_type STRING,
    `timestamp` BIGINT,
    event_time TIMESTAMP,
    event_date STRING,
    event_hour INT,
    weekday INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana/user_behavior'
TBLPROPERTIES ("skip.header.line.count"="1");

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
