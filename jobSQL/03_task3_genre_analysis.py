#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
任务3: 类型分析 - 电影类型分布和评分
使用 PySpark 分析各类型的电影数量、平均评分
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, stddev, min, max, round as spark_round, explode, split

def main():
    spark = SparkSession.builder \
        .appName("Task3_GenreAnalysis") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("任务3: 类型分析")
    print("=" * 60)

    movies_df = spark.read.csv("dataset/movies.csv", header=True, inferSchema=True)
    ratings_df = spark.read.csv("dataset/ratings.csv", header=True, inferSchema=True)

    movies_with_genre = movies_df \
        .withColumn("genre", explode(split("genres", "\\|")))

    joined_df = ratings_df.join(movies_with_genre, "movieId")

    genre_stats = joined_df.groupBy("genre") \
        .agg(
            count("movieId").alias("movie_count"),
            count("rating").alias("total_ratings"),
            spark_round(avg("rating"), 2).alias("avg_rating"),
            spark_round(stddev("rating"), 2).alias("rating_stddev"),
            min("rating").alias("min_rating"),
            max("rating").alias("max_rating")
        ) \
        .orderBy("movie_count", ascending=False)

    print("\n--- 电影类型分布统计 ---")
    genre_stats.show(20, truncate=False)

    genre_stats.write.mode("overwrite").csv("output/task3_genre_stats", header=True)

    genre_combination = ratings_df.join(movies_df, "movieId") \
        .groupBy("genres") \
        .agg(
            count("movieId").alias("movie_count"),
            spark_round(avg("rating"), 2).alias("avg_rating")
        ) \
        .filter("movie_count >= 5") \
        .orderBy("avg_rating", ascending=False)

    print("\n--- 类型组合评分 (Top 20) ---")
    genre_combination.show(20, truncate=False)

    genre_combination.write.mode("overwrite").csv("output/task3_genre_combination", header=True)
    print(f"\n结果已保存到 output/")

    spark.stop()

if __name__ == "__main__":
    main()
