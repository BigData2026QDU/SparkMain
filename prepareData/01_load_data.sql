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
