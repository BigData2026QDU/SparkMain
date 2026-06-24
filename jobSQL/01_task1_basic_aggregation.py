#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
任务1: 基础聚合分析 - 电影平均评分统计
使用 PySpark 直接读取 CSV 文件运行分析
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, max, min, stddev, round as spark_round

def main():
    spark = SparkSession.builder \
        .appName("Task1_BasicAggregation") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("任务1: 基础聚合分析")
    print("=" * 60)

    movies_df = spark.read.csv("dataset/movies.csv", header=True, inferSchema=True)
    ratings_df = spark.read.csv("dataset/ratings.csv", header=True, inferSchema=True)

    print(f"\n电影数量: {movies_df.count()}")
    print(f"评分数量: {ratings_df.count()}")

    result_df = ratings_df.groupBy("movieId") \
        .agg(
            count("rating").alias("rating_count"),
            spark_round(avg("rating"), 2).alias("avg_rating"),
            max("rating").alias("max_rating"),
            min("rating").alias("min_rating"),
            spark_round(stddev("rating"), 2).alias("rating_stddev")
        ) \
        .filter("rating_count >= 10") \
        .join(movies_df, "movieId") \
        .select("movieId", "title", "genres", "rating_count", "avg_rating",
                "max_rating", "min_rating", "rating_stddev") \
        .orderBy("avg_rating", ascending=False)

    print("\n--- 电影平均评分统计 (Top 20) ---")
    result_df.show(20, truncate=False)

    result_df.write.mode("overwrite").csv("output/task1_movie_stats", header=True)
    print(f"\n结果已保存到 output/task1_movie_stats")

    spark.stop()

if __name__ == "__main__":
    main()
