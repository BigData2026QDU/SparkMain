-- 测试: 创建 Hive 表
-- 设置 Hive 执行引擎为 Spark
SET hive.execution.engine=spark;
SET spark.master=local[*];

-- 创建测试数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS bigdata_ana_test;

USE bigdata_ana_test;

-- 创建电影表
DROP TABLE IF EXISTS movies;
CREATE EXTERNAL TABLE movies (
    movieId INT,
    title STRING,
    genres STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana_test/movies'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 创建评分表
DROP TABLE IF EXISTS ratings;
CREATE EXTERNAL TABLE ratings (
    userId INT,
    movieId INT,
    rating DOUBLE,
    `timestamp` BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana_test/ratings'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 创建标签表
DROP TABLE IF EXISTS tags;
CREATE EXTERNAL TABLE tags (
    userId INT,
    movieId INT,
    tag STRING,
    `timestamp` BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana_test/tags'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 创建链接表
DROP TABLE IF EXISTS links;
CREATE EXTERNAL TABLE links (
    movieId INT,
    imdbId STRING,
    tmdbId INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana_test/links'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS user_behavior;
CREATE EXTERNAL TABLE user_behavior (
    user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior_type STRING,
    `timestamp` BIGINT,
    event_time STRING,
    event_date STRING,
    event_hour INT,
    weekday INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana_test/user_behavior'
TBLPROPERTIES ("skip.header.line.count"="1");
