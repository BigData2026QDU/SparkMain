package org.bigdata.analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object AnalyzeUsers {
  private val TaskName = "users"
  private val MySQLTableEnv = "MYSQL_TABLE_USERS"

  def main(args: Array[String]): Unit = {
    val config = MovieLensAnalysisSupport.parseArgs(args, TaskName)
    val spark = MovieLensAnalysisSupport.spark("AnalyzeUsers")

    try {
      val report =
        buildReport(MovieLensAnalysisSupport.readRatings(spark, config.inputDir))

      MovieLensAnalysisSupport.writeReport(report, config)
      MovieLensAnalysisSupport.exportReportIfConfigured(report, MySQLTableEnv)
    } finally {
      spark.stop()
    }
  }

  private[analysis] def buildReport(ratings: DataFrame): DataFrame = {
    val ratingDate = to_date(to_timestamp(from_unixtime(col("timestamp"))))
    val userStats = ratings
      .groupBy(col("userId"))
      .agg(
        count(lit(1)).as("rating_count"),
        round(avg(col("rating")), 3).as("avg_rating"),
        countDistinct(col("movieId")).as("unique_movies"),
        countDistinct(ratingDate).as("active_days"))

    val activityWindow = Window.orderBy(col("rating_count"), col("userId"))
    val preferenceWindow = Window.orderBy(col("avg_rating"), col("userId"))

    userStats
      .withColumn("activity_rank", ntile(4).over(activityWindow))
      .withColumn("preference_rank", ntile(3).over(preferenceWindow))
      .withColumn(
        "activity_segment",
        when(col("activity_rank") === 1, "light")
          .when(col("activity_rank") === 2, "regular")
          .when(col("activity_rank") === 3, "active")
          .otherwise("power"))
      .withColumn(
        "rating_tendency",
        when(col("preference_rank") === 1, "critical")
          .when(col("preference_rank") === 2, "balanced")
          .otherwise("positive"))
      .groupBy(col("activity_segment"), col("rating_tendency"))
      .agg(
        count(lit(1)).as("user_count"),
        round(avg(col("rating_count")), 2).as("avg_ratings_per_user"),
        round(avg(col("avg_rating")), 3).as("avg_user_rating"),
        round(avg(col("unique_movies")), 2).as("avg_unique_movies"),
        round(avg(col("active_days")), 2).as("avg_active_days"))
      .orderBy(desc("user_count"), asc("activity_segment"), asc("rating_tendency"))
  }
}
