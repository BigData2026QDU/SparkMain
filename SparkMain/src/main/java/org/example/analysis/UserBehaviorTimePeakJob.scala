package org.example.analysis

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * LuckyAnJun issue #16: 24-hour traffic and purchase-rate distribution.
 *
 * Usage:
 *   spark-submit --class org.example.analysis.UserBehaviorTimePeakJob <jar> \
 *     [database] [sourceTable] [outputBasePath]
 */
object UserBehaviorTimePeakJob {
  def main(args: Array[String]): Unit = {
    val database = args.lift(0).getOrElse("bigdata_ana")
    val sourceTable = args.lift(1).getOrElse("dwd_user_behavior_clean")
    val outputBasePath = args.lift(2).getOrElse("/user/hive/bigdata_ana")

    val spark = SparkSession.builder()
      .appName("LuckyAnJun-HourDistribution")
      .enableHiveSupport()
      .getOrCreate()

    try {
      spark.sql(s"USE $database")

      val events = spark.table(sourceTable)
        .select(
          col("user_id").cast("long").as("user_id"),
          lower(col("behavior_type")).as("behavior_type"),
          col("event_hour").cast("int").as("event_hour"))
        .filter(col("user_id").isNotNull && col("user_id") > 0)
        .filter(col("event_hour").isNotNull && col("event_hour").between(0, 23))
        .filter(col("behavior_type").isin("pv", "fav", "cart", "buy"))

      val hourDistribution = events
        .groupBy("event_hour")
        .agg(
          count(when(col("behavior_type") === "pv", 1)).cast("long").as("pv"),
          countDistinct(when(col("behavior_type") === "pv", col("user_id"))).cast("long").as("pv_uv"),
          count(when(col("behavior_type") === "fav", 1)).cast("long").as("fav_cnt"),
          count(when(col("behavior_type") === "cart", 1)).cast("long").as("cart_cnt"),
          count(when(col("behavior_type") === "buy", 1)).cast("long").as("buy_cnt"),
          countDistinct(when(col("behavior_type") === "buy", col("user_id"))).cast("long").as("buy_uv"))
        .filter(col("pv_uv") > 0)
        .withColumn(
          "hourly_buy_rate",
          round(col("buy_uv").cast("double") / col("pv_uv").cast("double"), 4))

      writeResultTable(
        spark,
        hourDistribution,
        database,
        "lb_time_hour_distribution",
        s"$outputBasePath/lb_time_hour_distribution")

      println("[SUCCESS] Hour distribution completed")
      hourDistribution.orderBy("event_hour").show(24, truncate = false)
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
