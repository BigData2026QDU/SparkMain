package org.bigdata.analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object AnalyzeTime {
  private val TaskName = "time"
  private val MySQLTableEnv = "MYSQL_TABLE_TIME"

  def main(args: Array[String]): Unit = {
    val config = MovieLensAnalysisSupport.parseArgs(args, TaskName)
    val spark = MovieLensAnalysisSupport.spark("AnalyzeTime")

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
    val withTime = ratings
      .withColumn(
        "rating_timestamp",
        to_timestamp(from_unixtime(col("timestamp"))))
      .withColumn("rating_month", date_format(col("rating_timestamp"), "yyyy-MM"))

    withTime
      .groupBy(col("rating_month"))
      .agg(
        count(lit(1)).as("rating_count"),
        countDistinct(col("userId")).as("active_users"),
        countDistinct(col("movieId")).as("rated_movies"),
        round(avg(col("rating")), 3).as("avg_rating"),
        min(col("rating_timestamp")).as("first_rating_at"),
        max(col("rating_timestamp")).as("last_rating_at"))
      .orderBy(asc("rating_month"))
  }
}
