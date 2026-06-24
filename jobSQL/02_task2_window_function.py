#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
任务2: 窗口函数分析 - 电影排名和百分比
使用 PySpark 窗口函数计算电影排名
"""

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import count, avg, round as spark_round, row_number, rank, percent_rank, cume_dist

def main():
    spark = SparkSession.builder \
        .appName("Task2_WindowFunction") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("任务2: 窗口函数分析")
    print("=" * 60)

    movies_df = spark.read.csv("dataset/movies.csv", header=True, inferSchema=True)
    ratings_df = spark.read.csv("dataset/ratings.csv", header=True, inferSchema=True)

    movie_stats = ratings_df.groupBy("movieId") \
        .agg(
            count("rating").alias("rating_count"),
            spark_round(avg("rating"), 2).alias("avg_rating")
        ) \
        .filter("rating_count >= 5") \
        .join(movies_df, "movieId")

    window_spec = Window.orderBy("avg_rating")

    result_df = movie_stats \
        .withColumn("row_num", row_number().over(window_spec)) \
        .withColumn("rank_num", rank().over(window_spec)) \
        .withColumn("percentile", spark_round(percent_rank().over(window_spec) * 100, 2)) \
        .withColumn("cumulative_dist", spark_round(cume_dist().over(window_spec) * 100, 2))

    print("\n--- 电影排名分析 (Top 20) ---")
    result_df.select("movieId", "title", "genres", "rating_count", "avg_rating",
                     "row_num", "rank_num", "percentile") \
        .orderBy("avg_rating", ascending=False) \
        .show(20, truncate=False)

    result_df.write.mode("overwrite").csv("output/task2_movie_ranking", header=True)
    print(f"\n结果已保存到 output/task2_movie_ranking")

    spark.stop()

if __name__ == "__main__":
    main()
