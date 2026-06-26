-- 测试任务: 基础聚合分析


USE bigdata_ana_test;

-- 创建结果表
DROP TABLE IF EXISTS task1_movie_stats;
CREATE TABLE task1_movie_stats AS
SELECT
    m.movieId,
    m.title,
    m.genres,
    COUNT(r.rating) AS rating_count,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    MAX(r.rating) AS max_rating,
    MIN(r.rating) AS min_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.movieId, m.title, m.genres
HAVING COUNT(r.rating) >= 2
ORDER BY avg_rating DESC;

-- 查询结果
SELECT * FROM task1_movie_stats LIMIT 20;
