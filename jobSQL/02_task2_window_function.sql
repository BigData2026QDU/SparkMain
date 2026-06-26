-- 任务2: 窗口函数分析 - 电影排名和百分比
-- 展示 ROW_NUMBER, RANK, PERCENT_RANK 的使用


USE bigdata_ana;

-- 创建结果表
DROP TABLE IF EXISTS task2_movie_ranking;
CREATE TABLE task2_movie_ranking AS
WITH movie_stats AS (
    SELECT
        m.movieId,
        m.title,
        m.genres,
        COUNT(r.rating) AS rating_count,
        AVG(r.rating) AS avg_rating
    FROM movies m
    JOIN ratings r ON m.movieId = r.movieId
    GROUP BY m.movieId, m.title, m.genres
    HAVING COUNT(r.rating) >= 5
)
SELECT
    movieId,
    title,
    genres,
    rating_count,
    ROUND(avg_rating, 2) AS avg_rating,
    ROW_NUMBER() OVER (ORDER BY avg_rating DESC) AS row_num,
    RANK() OVER (ORDER BY avg_rating DESC) AS rank_num,
    ROUND(PERCENT_RANK() OVER (ORDER BY avg_rating ASC) * 100, 2) AS percentile,
    ROUND(CUME_DIST() OVER (ORDER BY avg_rating ASC) * 100, 2) AS cumulative_dist
FROM movie_stats;

-- 查询结果
SELECT * FROM task2_movie_ranking LIMIT 20;
