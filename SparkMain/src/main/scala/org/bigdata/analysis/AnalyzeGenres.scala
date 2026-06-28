package org.bigdata.analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object AnalyzeGenres {
  private val TaskName = "genres"
  private val MySQLTableEnv = "MYSQL_TABLE_GENRES"

  def main(args: Array[String]): Unit = {
    val config = MovieLensAnalysisSupport.parseArgs(args, TaskName)
    val spark = MovieLensAnalysisSupport.spark("AnalyzeGenres")

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
    val totalRatings = ratings.count()
    val ratingShare =
      if (totalRatings == 0) lit(0.0)
      else round(col("rating_count").cast("double") / lit(totalRatings.toDouble), 4)

    val movieGenres = movies
      .withColumn("genre", explode(split(col("genres"), "\\|")))
      .filter(col("genre").isNotNull && col("genre") =!= "(no genres listed)")
      .select("movieId", "title", "genre")

    ratings
      .join(movieGenres, Seq("movieId"), "inner")
      .groupBy(col("genre"))
      .agg(
        count(lit(1)).as("rating_count"),
        countDistinct(col("movieId")).as("movie_count"),
        countDistinct(col("userId")).as("user_count"),
        round(avg(col("rating")), 3).as("avg_rating"))
      .withColumn("rating_share", ratingShare)
      .select(
        col("genre"),
        col("rating_count"),
        col("movie_count"),
        col("user_count"),
        col("avg_rating"),
        col("rating_share"))
      .orderBy(desc("rating_count"), desc("avg_rating"), asc("genre"))
  }
}
