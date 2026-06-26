-- Spark SQL source table schema.
-- The main pipeline loads CSV files directly and overwrites these tables.

CREATE DATABASE IF NOT EXISTS bigdata_ana;

USE bigdata_ana;

DROP TABLE IF EXISTS movies;
CREATE TABLE movies (
    movieId INT,
    title STRING,
    genres STRING
) USING parquet;

DROP TABLE IF EXISTS ratings;
CREATE TABLE ratings (
    userId INT,
    movieId INT,
    rating DOUBLE,
    `timestamp` BIGINT
) USING parquet;

DROP TABLE IF EXISTS tags;
CREATE TABLE tags (
    userId INT,
    movieId INT,
    tag STRING,
    `timestamp` BIGINT
) USING parquet;

DROP TABLE IF EXISTS links;
CREATE TABLE links (
    movieId INT,
    imdbId STRING,
    tmdbId INT
) USING parquet;
