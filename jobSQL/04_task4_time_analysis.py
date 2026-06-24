#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
任务4: 时间分析 - 评分时间趋势
使用 PySpark 分析评分的时间分布、年度趋势
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, round as spark_round, from_unixtime, year, month, dayofweek, \
    countDistinct, datediff, min as spark_min, max as spark_max, when, col

def main():
    spark = SparkSession.builder \
        .appName("Task4_TimeAnalysis") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("任务4: 时间分析")
    print("=" * 60)

    ratings_df = spark.read.csv("dataset/ratings.csv", header=True, inferSchema=True)

    ratings_with_time = ratings_df \
        .withColumn("rating_date", from_unixtime("timestamp")) \
        .withColumn("rating_year", year(from_unixtime("timestamp"))) \
        .withColumn("rating_month", month(from_unixtime("timestamp"))) \
        .withColumn("day_of_week", dayofweek(from_unixtime("timestamp")))

    print("\n--- 年度评分趋势 ---")
    yearly_trend = ratings_with_time.groupBy("rating_year") \
        .agg(
            count("rating").alias("total_ratings"),
            countDistinct("userId").alias("active_users"),
            countDistinct("movieId").alias("rated_movies"),
            spark_round(avg("rating"), 2).alias("avg_rating")
        ) \
        .orderBy("rating_year")

    yearly_trend.show(20, truncate=False)
    yearly_trend.write.mode("overwrite").csv("output/task4_yearly_trend", header=True)

    print("\n--- 月度评分趋势 (最近24个月) ---")
    monthly_trend = ratings_with_time.groupBy("rating_year", "rating_month") \
        .agg(
            count("rating").alias("total_ratings"),
            spark_round(avg("rating"), 2).alias("avg_rating")
        ) \
        .orderBy("rating_year", "rating_month")

    monthly_trend.show(24, truncate=False)
    monthly_trend.write.mode("overwrite").csv("output/task4_monthly_trend", header=True)

    print("\n--- 星期分布 ---")
    weekday_names = {1: "周日", 2: "周一", 3: "周二", 4: "周三", 5: "周四", 6: "周五", 7: "周六"}
    weekday_dist = ratings_with_time.groupBy("day_of_week") \
        .agg(
            count("rating").alias("total_ratings"),
            spark_round(avg("rating"), 2).alias("avg_rating")
        ) \
        .withColumn("day_name",
            when(col("day_of_week") == 1, "周日")
            .when(col("day_of_week") == 2, "周一")
            .when(col("day_of_week") == 3, "周二")
            .when(col("day_of_week") == 4, "周三")
            .when(col("day_of_week") == 5, "周四")
            .when(col("day_of_week") == 6, "周五")
            .when(col("day_of_week") == 7, "周六")
        ) \
        .orderBy("day_of_week")

    weekday_dist.show(20, truncate=False)
    weekday_dist.write.mode("overwrite").csv("output/task4_weekday_distribution", header=True)

    print(f"\n结果已保存到 output/")

    spark.stop()

if __name__ == "__main__":
    main()
