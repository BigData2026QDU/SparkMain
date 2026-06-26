package org.example.analysis

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Scala Spark implementation for LuckyAnJun issue #16.
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
      .appName("LuckyAnJun-UserBehaviorTimePeakJob")
      .enableHiveSupport()
      .getOrCreate()

    try {
      spark.sql(s"USE $database")

      val dwd = spark.table(sourceTable)
        .select(
          col("user_id").cast("long").as("user_id"),
          lower(col("behavior_type")).as("behavior_type"),
          col("event_date").cast("string").as("event_date"),
          col("event_hour").cast("int").as("event_hour"),
          col("weekday").cast("int").as("weekday")
        )
        .filter(col("user_id").isNotNull)
        .filter(col("event_date").isNotNull && col("event_date") =!= "event_date")
        .filter(col("event_hour").isNotNull && col("event_hour").between(0, 23))
        .filter(col("weekday").isNotNull && col("weekday").between(1, 7))
        .filter(col("behavior_type").isin("pv", "fav", "cart", "buy"))

      val hourly = addBuyRate(
        dwd.groupBy("event_date", "event_hour")
          .agg(
            max(col("weekday")).as("weekday"),
            count(when(col("behavior_type") === "pv", 1)).cast("long").as("pv"),
            countDistinct(when(col("behavior_type") === "pv", col("user_id"))).cast("long").as("pv_uv"),
            count(when(col("behavior_type") === "fav", 1)).cast("long").as("fav_cnt"),
            count(when(col("behavior_type") === "cart", 1)).cast("long").as("cart_cnt"),
            count(when(col("behavior_type") === "buy", 1)).cast("long").as("buy_cnt"),
            countDistinct(when(col("behavior_type") === "buy", col("user_id"))).cast("long").as("buy_uv")
          )
          .filter(col("pv_uv") > 0)
      ).select(
        col("event_date"),
        col("event_hour"),
        col("weekday"),
        col("pv"),
        col("pv_uv"),
        col("fav_cnt"),
        col("cart_cnt"),
        col("buy_cnt"),
        col("buy_uv"),
        col("hourly_buy_rate")
      )

      val hourDistribution = addBuyRate(
        dwd.groupBy("event_hour")
          .agg(
            count(when(col("behavior_type") === "pv", 1)).cast("long").as("pv"),
            countDistinct(when(col("behavior_type") === "pv", col("user_id"))).cast("long").as("pv_uv"),
            count(when(col("behavior_type") === "fav", 1)).cast("long").as("fav_cnt"),
            count(when(col("behavior_type") === "cart", 1)).cast("long").as("cart_cnt"),
            count(when(col("behavior_type") === "buy", 1)).cast("long").as("buy_cnt"),
            countDistinct(when(col("behavior_type") === "buy", col("user_id"))).cast("long").as("buy_uv")
          )
          .filter(col("pv_uv") > 0)
      )

      val weekdayHourHeatmap = addBuyRate(
        dwd.groupBy("weekday", "event_hour")
          .agg(
            count(when(col("behavior_type") === "pv", 1)).cast("long").as("pv"),
            countDistinct(when(col("behavior_type") === "pv", col("user_id"))).cast("long").as("pv_uv"),
            count(when(col("behavior_type") === "buy", 1)).cast("long").as("buy_cnt"),
            countDistinct(when(col("behavior_type") === "buy", col("user_id"))).cast("long").as("buy_uv")
          )
          .filter(col("pv_uv") > 0)
      ).withColumnRenamed("hourly_buy_rate", "buy_rate")

      val highConversion = hourly
        .filter(col("pv_uv") >= 100)
        .orderBy(col("hourly_buy_rate").desc, col("buy_uv").desc, col("pv").desc)
        .limit(10)

      val lowConversion = hourly
        .withColumn("avg_pv", avg(col("pv")).over(Window.partitionBy()))
        .filter(col("pv") >= col("avg_pv"))
        .drop("avg_pv")
        .orderBy(col("hourly_buy_rate").asc, col("pv").desc)
        .limit(10)

      val resultTables = Seq(
        ResultTable(
          "lb_time_hourly_behavior",
          s"$outputBasePath/lb_time_hourly_behavior",
          hourly,
          """
            |event_date STRING,
            |event_hour INT,
            |weekday INT,
            |pv BIGINT,
            |pv_uv BIGINT,
            |fav_cnt BIGINT,
            |cart_cnt BIGINT,
            |buy_cnt BIGINT,
            |buy_uv BIGINT,
            |hourly_buy_rate DOUBLE
            |""".stripMargin),
        ResultTable(
          "lb_time_hour_distribution",
          s"$outputBasePath/lb_time_hour_distribution",
          hourDistribution,
          """
            |event_hour INT,
            |pv BIGINT,
            |pv_uv BIGINT,
            |fav_cnt BIGINT,
            |cart_cnt BIGINT,
            |buy_cnt BIGINT,
            |buy_uv BIGINT,
            |hourly_buy_rate DOUBLE
            |""".stripMargin),
        ResultTable(
          "lb_time_weekday_hour_heatmap",
          s"$outputBasePath/lb_time_weekday_hour_heatmap",
          weekdayHourHeatmap,
          """
            |weekday INT,
            |event_hour INT,
            |pv BIGINT,
            |pv_uv BIGINT,
            |buy_cnt BIGINT,
            |buy_uv BIGINT,
            |buy_rate DOUBLE
            |""".stripMargin),
        ResultTable(
          "lb_time_high_conversion_slots",
          s"$outputBasePath/lb_time_high_conversion_slots",
          highConversion,
          """
            |event_date STRING,
            |event_hour INT,
            |weekday INT,
            |pv BIGINT,
            |pv_uv BIGINT,
            |fav_cnt BIGINT,
            |cart_cnt BIGINT,
            |buy_cnt BIGINT,
            |buy_uv BIGINT,
            |hourly_buy_rate DOUBLE
            |""".stripMargin),
        ResultTable(
          "lb_time_low_conversion_slots",
          s"$outputBasePath/lb_time_low_conversion_slots",
          lowConversion,
          """
            |event_date STRING,
            |event_hour INT,
            |weekday INT,
            |pv BIGINT,
            |pv_uv BIGINT,
            |fav_cnt BIGINT,
            |cart_cnt BIGINT,
            |buy_cnt BIGINT,
            |buy_uv BIGINT,
            |hourly_buy_rate DOUBLE
            |""".stripMargin)
      )

      resultTables.foreach(createExternalParquetTable(spark, database, _))
      resultTables.foreach(writeParquetResult)

      println("[SUCCESS] Time peak analysis completed")
      hourly.orderBy("event_date", "event_hour").show(24, truncate = false)
      hourDistribution.orderBy("event_hour").show(24, truncate = false)
      weekdayHourHeatmap.orderBy("weekday", "event_hour").show(50, truncate = false)
      highConversion.show(10, truncate = false)
      lowConversion.show(10, truncate = false)
    } finally {
      spark.stop()
    }
  }

  private def addBuyRate(df: DataFrame): DataFrame = {
    df.withColumn(
      "hourly_buy_rate",
      round(
        when(col("pv_uv") === 0, lit(0.0))
          .otherwise(col("buy_uv").cast("double") / col("pv_uv").cast("double")),
        4
      )
    )
  }

  private case class ResultTable(
      tableName: String,
      outputPath: String,
      df: DataFrame,
      schemaSql: String)

  private def createExternalParquetTable(
      spark: SparkSession,
      database: String,
      result: ResultTable): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $database.${result.tableName}")
    spark.sql(
      s"""
         |CREATE EXTERNAL TABLE $database.${result.tableName} (
         |${result.schemaSql}
         |)
         |STORED AS PARQUET
         |LOCATION '${result.outputPath}'
         |""".stripMargin)
  }

  private def writeParquetResult(result: ResultTable): Unit = {
    result.df.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save(result.outputPath)
  }
}
