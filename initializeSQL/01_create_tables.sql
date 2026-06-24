-- 创建 Hive 数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS bigdata_ana;

USE bigdata_ana;

-- 创建电影表
DROP TABLE IF EXISTS movies;
CREATE TABLE movies (
    movieId INT,
    title STRING,
    genres STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- 创建评分表
DROP TABLE IF EXISTS ratings;
CREATE TABLE ratings (
    userId INT,
    movieId INT,
    rating DOUBLE,
    `timestamp` BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- 创建标签表
DROP TABLE IF EXISTS tags;
CREATE TABLE tags (
    userId INT,
    movieId INT,
    tag STRING,
    `timestamp` BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- 创建链接表
DROP TABLE IF EXISTS links;
CREATE TABLE links (
    movieId INT,
    imdbId STRING,
    tmdbId INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");
