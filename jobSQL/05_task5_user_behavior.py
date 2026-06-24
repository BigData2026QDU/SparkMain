#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
任务5: 用户行为分析 - 活跃用户和偏好
使用 PySpark 分析用户评分行为、活跃度、偏好类型
"""

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import count as spark_count, avg, round as spark_round, countDistinct, \
    from_unixtime, min as spark_min, max as spark_max, datediff, row_number

def main():
    spark = SparkSession.builder \
        .appName("Task5_UserBehavior") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("任务5: 用户行为分析")
    print("=" * 60)

    movies_df = spark.read.csv("dataset/movies.csv", header=True, inferSchema=True)
    ratings_df = spark.read.csv("dataset/ratings.csv", header=True, inferSchema=True)

    print("\n--- 用户活跃度统计 (Top 20) ---")
    user_activity = ratings_df.groupBy("userId") \
        .agg(
            spark_count("rating").alias("total_ratings"),
            countDistinct("movieId").alias("unique_movies"),
            spark_round(avg("rating"), 2).alias("avg_rating_given"),
            spark_min(from_unixtime("timestamp")).alias("first_rating_time"),
            spark_max(from_unixtime("timestamp")).alias("last_rating_time")
        )

    user_activity = user_activity \
        .withColumn("active_days",
            datediff(
                spark_max(from_unixtime("timestamp")),
                spark_min(from_unixtime("timestamp"))
            )
        )

    user_activity.orderBy("total_ratings", ascending=False).show(20, truncate=False)
    user_activity.write.mode("overwrite").csv("output/task5_user_activity", header=True)

    print("\n--- 用户偏好类型 (Top 20) ---")
    user_genre_stats = ratings_df.join(movies_df, "movieId") \
        .groupBy("userId", "genres") \
        .agg(
            spark_count("rating").alias("rating_count"),
            spark_round(avg("rating"), 2).alias("avg_rating")
        )

    window_spec = Window.partitionBy("userId").orderBy("rating_count")

    user_preference = user_genre_stats \
        .withColumn("genre_rank", row_number().over(window_spec)) \
        .filter("genre_rank = 1") \
        .select("userId", "genres", "rating_count", "avg_rating")

    user_preference.orderBy("rating_count", ascending=False).show(20, truncate=False)
    user_preference.write.mode("overwrite").csv("output/task5_user_preference", header=True)

    print("\n--- 评分分布 ---")
    total_count = ratings_df.count()
    rating_dist = ratings_df.groupBy("rating") \
        .agg(spark_count("rating").alias("count")) \
        .withColumn("percentage",
            spark_round(
                spark_count("rating") / total_count * 100, 2
            )
        ) \
        .orderBy("rating")

    rating_dist.show(20, truncate=False)
    rating_dist.write.mode("overwrite").csv("output/task5_rating_distribution", header=True)

    print(f"\n结果已保存到 output/")

    spark.stop()

if __name__ == "__main__":
    main()
