-- 任务3: 类型分析 - 电影类型分布和评分
-- 展示 LATERAL VIEW 和字符串处理的使用


USE bigdata_ana;

-- 创建结果表
DROP TABLE IF EXISTS task3_genre_stats;
CREATE TABLE task3_genre_stats AS
SELECT
    genre,
    COUNT(DISTINCT movieId) AS movie_count,
    COUNT(*) AS total_ratings,
    ROUND(AVG(rating), 2) AS avg_rating,
    ROUND(STDDEV(rating), 2) AS rating_stddev,
    MIN(rating) AS min_rating,
    MAX(rating) AS max_rating
FROM ratings r
LATERAL VIEW EXPLODE(SPLIT(r2.genres, '\\|')) t AS genre
JOIN movies r2 ON r.movieId = r2.movieId
GROUP BY genre
ORDER BY movie_count DESC;

-- 查询结果
SELECT * FROM task3_genre_stats;

-- 创建类型组合分析表
DROP TABLE IF EXISTS task3_genre_combination;
CREATE TABLE task3_genre_combination AS
SELECT
    genres,
    COUNT(*) AS movie_count,
    ROUND(AVG(r.rating), 2) AS avg_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY genres
HAVING COUNT(*) >= 5
ORDER BY avg_rating DESC;

SELECT * FROM task3_genre_combination LIMIT 20;
