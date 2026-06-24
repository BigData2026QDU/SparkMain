-- 任务4: 时间分析 - 评分时间趋势
-- 分析评分的时间分布、年度趋势、活跃时段
-- 展示日期函数和时间窗口分析

USE bigdata_ana;

-- 创建结果表：年度评分趋势
DROP TABLE IF EXISTS task4_yearly_trend;
CREATE TABLE task4_yearly_trend AS
SELECT
    YEAR(FROM_UNIXTIME(`timestamp`)) AS rating_year,
    COUNT(*) AS total_ratings,
    COUNT(DISTINCT userId) AS active_users,
    COUNT(DISTINCT movieId) AS rated_movies,
    ROUND(AVG(rating), 2) AS avg_rating
FROM ratings
GROUP BY YEAR(FROM_UNIXTIME(`timestamp`))
ORDER BY rating_year;

-- 查询结果
SELECT * FROM task4_yearly_trend;

-- 创建结果表：月度评分趋势
DROP TABLE IF EXISTS task4_monthly_trend;
CREATE TABLE task4_monthly_trend AS
SELECT
    YEAR(FROM_UNIXTIME(`timestamp`)) AS rating_year,
    MONTH(FROM_UNIXTIME(`timestamp`)) AS rating_month,
    COUNT(*) AS total_ratings,
    ROUND(AVG(rating), 2) AS avg_rating
FROM ratings
GROUP BY YEAR(FROM_UNIXTIME(`timestamp`)), MONTH(FROM_UNIXTIME(`timestamp`))
ORDER BY rating_year, rating_month;

-- 查询结果
SELECT * FROM task4_monthly_trend LIMIT 24;

-- 创建结果表：星期分布
DROP TABLE IF EXISTS task4_weekday_distribution;
CREATE TABLE task4_weekday_distribution AS
SELECT
    DAYOFWEEK(FROM_UNIXTIME(`timestamp`)) AS day_of_week,
    CASE DAYOFWEEK(FROM_UNIXTIME(`timestamp`))
        WHEN 1 THEN '周日'
        WHEN 2 THEN '周一'
        WHEN 3 THEN '周二'
        WHEN 4 THEN '周三'
        WHEN 5 THEN '周四'
        WHEN 6 THEN '周五'
        WHEN 7 THEN '周六'
    END AS day_name,
    COUNT(*) AS total_ratings,
    ROUND(AVG(rating), 2) AS avg_rating
FROM ratings
GROUP BY DAYOFWEEK(FROM_UNIXTIME(`timestamp`))
ORDER BY day_of_week;

-- 查询结果
SELECT * FROM task4_weekday_distribution;
