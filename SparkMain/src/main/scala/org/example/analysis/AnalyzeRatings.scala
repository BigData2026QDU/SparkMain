package org.example.analysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 评分分析任务
 * 从 Parquet 文件读取数据，进行分析并输出结果
 */
object AnalyzeRatings {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AnalyzeRatings")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    // 读取评分数据
    val ratingsPath = if (args.length > 0) args(0) else "output/ratings_streaming"
    val ratings = spark.read.parquet(ratingsPath)

    // 创建临时视图
    ratings.createOrReplaceTempView("ratings")

    // 分析1: 电影平均评分统计
    val movieStats = spark.sql("""
      SELECT 
        movieId,
        COUNT(*) as rating_count,
        ROUND(AVG(rating), 2) as avg_rating,
        MAX(rating) as max_rating,
        MIN(rating) as min_rating
      FROM ratings
      GROUP BY movieId
      HAVING COUNT(*) >= 2
      ORDER BY avg_rating DESC
    """)

    // 输出结果
    movieStats.show(20, truncate = false)

    // 保存结果
    movieStats.write.mode("overwrite").parquet("output/movie_stats")

    println("评分分析完成")
    spark.stop()
  }
}
