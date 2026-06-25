-- 测试任务3: 类型分析
-- 使用 Hive on Spark 执行

SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana_test;

-- 创建类型统计表
DROP TABLE IF EXISTS task3_genre_stats;
CREATE TABLE task3_genre_stats AS
SELECT
    genre,
    COUNT(*) AS movie_count,
    COUNT(DISTINCT m.movieId) AS unique_movies
FROM movies m
LATERAL VIEW EXPLODE(SPLIT(m.genres, '\\|')) t AS genre
GROUP BY genre
ORDER BY movie_count DESC;

-- 查询结果
SELECT * FROM task3_genre_stats;
