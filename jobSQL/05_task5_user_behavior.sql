-- 任务5: 用户行为分析 - 活跃用户和偏好
-- 分析用户评分行为、活跃度、偏好类型
-- 展示复杂子查询和用户画像分析

USE bigdata_ana;

-- 创建结果表：用户活跃度统计
DROP TABLE IF EXISTS task5_user_activity;
CREATE TABLE task5_user_activity AS
SELECT
    userId,
    COUNT(*) AS total_ratings,
    COUNT(DISTINCT movieId) AS unique_movies,
    ROUND(AVG(rating), 2) AS avg_rating_given,
    MIN(FROM_UNIXTIME(`timestamp`)) AS first_rating_time,
    MAX(FROM_UNIXTIME(`timestamp`)) AS last_rating_time,
    DATEDIFF(MAX(FROM_UNIXTIME(`timestamp`)), MIN(FROM_UNIXTIME(`timestamp`))) AS active_days
FROM ratings
GROUP BY userId;

-- 查询结果：最活跃用户
SELECT * FROM task5_user_activity ORDER BY total_ratings DESC LIMIT 20;

-- 创建结果表：用户偏好类型
DROP TABLE IF EXISTS task5_user_preference;
CREATE TABLE task5_user_preference AS
WITH user_genre_stats AS (
    SELECT
        r.userId,
        m.genres,
        COUNT(*) AS rating_count,
        AVG(r.rating) AS avg_rating
    FROM ratings r
    JOIN movies m ON r.movieId = m.movieId
    GROUP BY r.userId, m.genres
),
user_genre_rank AS (
    SELECT
        userId,
        genres,
        rating_count,
        avg_rating,
        ROW_NUMBER() OVER (PARTITION BY userId ORDER BY rating_count DESC) AS genre_rank
    FROM user_genre_stats
)
SELECT
    userId,
    genres AS preferred_genre,
    rating_count,
    ROUND(avg_rating, 2) AS avg_rating
FROM user_genre_rank
WHERE genre_rank = 1;

-- 查询结果
SELECT * FROM task5_user_preference LIMIT 20;

-- 创建结果表：评分分布
DROP TABLE IF EXISTS task5_rating_distribution;
CREATE TABLE task5_rating_distribution AS
SELECT
    rating,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM ratings
GROUP BY rating
ORDER BY rating;

-- 查询结果
SELECT * FROM task5_rating_distribution;
