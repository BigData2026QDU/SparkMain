package org.bigdata.analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object AnalyzeRatings {
  private val TaskName = "ratings"
  private val MySQLTableEnv = "MYSQL_TABLE_RATINGS"

  def main(args: Array[String]): Unit = {
    val config = MovieLensAnalysisSupport.parseArgs(args, TaskName)
    val spark = MovieLensAnalysisSupport.spark("AnalyzeRatings")

    try {
      val report = buildReport(
        MovieLensAnalysisSupport.readRatings(spark, config.inputDir),
        MovieLensAnalysisSupport.readMovies(spark, config.inputDir))

      MovieLensAnalysisSupport.writeReport(report, config)
      MovieLensAnalysisSupport.exportReportIfConfigured(report, MySQLTableEnv)
    } finally {
      spark.stop()
    }
  }

  private[analysis] def buildReport(
      ratings: DataFrame,
      movies: DataFrame): DataFrame = {
    val ratingStats = ratings
      .groupBy(col("movieId"))
      .agg(
        count(lit(1)).as("rating_count"),
        round(avg(col("rating")), 3).as("avg_rating"),
        min(col("rating")).as("min_rating"),
        max(col("rating")).as("max_rating"),
        coalesce(round(stddev_samp(col("rating")), 3), lit(0.0))
          .as("rating_stddev"))

    ratingStats
      .join(movies.select("movieId", "title", "genres"), Seq("movieId"), "left")
      .select(
        col("movieId"),
        col("title"),
        col("genres"),
        col("rating_count"),
        col("avg_rating"),
        col("min_rating"),
        col("max_rating"),
        col("rating_stddev"))
      .orderBy(desc("avg_rating"), desc("rating_count"), asc("movieId"))
  }
}
