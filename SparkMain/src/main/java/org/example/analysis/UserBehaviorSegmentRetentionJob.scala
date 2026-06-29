package org.example.analysis

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * LuckyAnJun issue #18: active-day distribution and behavior depth.
 *
 * Usage:
 *   spark-submit --class org.example.analysis.UserBehaviorSegmentRetentionJob <jar> \
 *     [database] [sourceTable] [outputBasePath]
 */
object UserBehaviorSegmentRetentionJob {
  def main(args: Array[String]): Unit = {
    val database = args.lift(0).getOrElse("bigdata_ana")
    val sourceTable = args.lift(1).getOrElse("dwd_user_behavior_clean")
    val outputBasePath = args.lift(2).getOrElse("/user/hive/bigdata_ana")

    val spark = SparkSession.builder()
      .appName("LuckyAnJun-ActiveDayDistribution")
      .enableHiveSupport()
      .getOrCreate()

    try {
      spark.sql(s"USE $database")

      val userActivity = spark.table(sourceTable)
        .select(
          col("user_id").cast("long").as("user_id"),
          col("item_id").cast("long").as("item_id"),
          lower(col("behavior_type")).as("behavior_type"),
          col("event_date").cast("string").as("event_date"))
        .filter(col("user_id").isNotNull && col("user_id") > 0)
        .filter(col("item_id").isNotNull && col("item_id") > 0)
        .filter(col("event_date").isNotNull && col("event_date") =!= "event_date")
        .filter(col("behavior_type").isin("pv", "fav", "cart", "buy"))
        .groupBy("user_id")
        .agg(
          countDistinct(col("event_date")).cast("long").as("active_days"),
          count(lit(1)).cast("long").as("behavior_cnt"),
          countDistinct(col("item_id")).cast("long").as("distinct_item_cnt"),
          count(when(col("behavior_type") === "buy", 1)).cast("long").as("buy_cnt"))

      val totalUsers = userActivity
        .agg(count(lit(1)).cast("long").as("total_users"))

      val activeDayDistribution = userActivity
        .groupBy("active_days")
        .agg(
          count(lit(1)).cast("long").as("user_cnt"),
          round(avg(col("behavior_cnt")), 2).as("avg_behavior_cnt"),
          round(avg(col("distinct_item_cnt")), 2).as("avg_distinct_item_cnt"),
          round(avg(col("buy_cnt")), 2).as("avg_buy_cnt"))
        .crossJoin(totalUsers)
        .withColumn(
          "user_rate",
          round(col("user_cnt").cast("double") / col("total_users").cast("double"), 4))
        .select(
          "active_days",
          "user_cnt",
          "user_rate",
          "avg_behavior_cnt",
          "avg_distinct_item_cnt",
          "avg_buy_cnt")

      writeResultTable(
        spark,
        activeDayDistribution,
        database,
        "lb_user_active_day_distribution",
        s"$outputBasePath/lb_user_active_day_distribution")

      println("[SUCCESS] Active-day distribution completed")
      activeDayDistribution.orderBy("active_days").show(20, truncate = false)
    } finally {
      spark.stop()
    }
  }

  private def writeResultTable(
      spark: SparkSession,
      df: org.apache.spark.sql.DataFrame,
      database: String,
      tableName: String,
      outputPath: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $database.$tableName")
    df.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .option("path", outputPath)
      .saveAsTable(s"$database.$tableName")
  }
}
