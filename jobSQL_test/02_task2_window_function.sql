-- 测试任务2: 窗口函数分析
-- 使用 Hive on Spark 执行

SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana_test;

-- 创建结果表
DROP TABLE IF EXISTS task2_movie_ranking;
CREATE TABLE task2_movie_ranking AS
WITH movie_stats AS (
    SELECT
        m.movieId,
        m.title,
        m.genres,
        COUNT(r.rating) AS rating_count,
        ROUND(AVG(r.rating), 2) AS avg_rating
    FROM movies m
    JOIN ratings r ON m.movieId = r.movieId
    GROUP BY m.movieId, m.title, m.genres
    HAVING COUNT(r.rating) >= 2
)
SELECT
    movieId,
    title,
    genres,
    rating_count,
    avg_rating,
    ROW_NUMBER() OVER (ORDER BY avg_rating DESC) AS ranking,
    RANK() OVER (ORDER BY avg_rating DESC) AS rank_with_ties,
    ROUND(PERCENT_RANK() OVER (ORDER BY avg_rating), 2) AS percentile
FROM movie_stats
ORDER BY ranking;

-- 查询结果
SELECT * FROM task2_movie_ranking LIMIT 20;
