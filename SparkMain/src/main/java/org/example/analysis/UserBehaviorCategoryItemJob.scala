package org.example.analysis

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Scala Spark implementation for LuckyAnJun issue #17.
 *
 * Usage:
 *   spark-submit --class org.example.analysis.UserBehaviorCategoryItemJob <jar> \
 *     [database] [sourceTable] [outputBasePath]
 */
object UserBehaviorCategoryItemJob {
  def main(args: Array[String]): Unit = {
    val database = args.lift(0).getOrElse("bigdata_ana")
    val sourceTable = args.lift(1).getOrElse("dwd_user_behavior_clean")
    val outputBasePath = args.lift(2).getOrElse("/user/hive/bigdata_ana")

    val spark = SparkSession.builder()
      .appName("LuckyAnJun-UserBehaviorCategoryItemJob")
      .enableHiveSupport()
      .getOrCreate()

    try {
      spark.sql(s"USE $database")

      val dwd = spark.table(sourceTable)
        .select(
          col("user_id").cast("long").as("user_id"),
          col("item_id").cast("long").as("item_id"),
          col("category_id").cast("long").as("category_id"),
          lower(col("behavior_type")).as("behavior_type")
        )
        .filter(col("user_id").isNotNull && col("user_id") > 0)
        .filter(col("item_id").isNotNull && col("item_id") > 0)
        .filter(col("category_id").isNotNull && col("category_id") > 0)
        .filter(col("behavior_type").isin("pv", "fav", "cart", "buy"))

      val categoryEfficiency = addConversionRate(
        dwd.groupBy("category_id")
          .agg(
            count(when(col("behavior_type") === "pv", 1)).cast("long").as("pv_cnt"),
            countDistinct(when(col("behavior_type") === "pv", col("user_id"))).cast("long").as("pv_uv"),
            count(when(col("behavior_type") === "fav", 1)).cast("long").as("fav_cnt"),
            count(when(col("behavior_type") === "cart", 1)).cast("long").as("cart_cnt"),
            count(when(col("behavior_type") === "buy", 1)).cast("long").as("buy_cnt"),
            countDistinct(when(col("behavior_type") === "buy", col("user_id"))).cast("long").as("buy_uv")
          )
          .filter(col("pv_uv") > 0),
        "category_conversion_rate"
      )

      val itemEfficiency = addConversionRate(
        dwd.groupBy("item_id", "category_id")
          .agg(
            count(when(col("behavior_type") === "pv", 1)).cast("long").as("pv_cnt"),
            countDistinct(when(col("behavior_type") === "pv", col("user_id"))).cast("long").as("pv_uv"),
            count(when(col("behavior_type") === "fav", 1)).cast("long").as("fav_cnt"),
            count(when(col("behavior_type") === "cart", 1)).cast("long").as("cart_cnt"),
            count(when(col("behavior_type").isin("fav", "cart"), 1)).cast("long").as("intent_cnt"),
            count(when(col("behavior_type") === "buy", 1)).cast("long").as("buy_cnt"),
            countDistinct(when(col("behavior_type") === "buy", col("user_id"))).cast("long").as("buy_uv")
          )
          .filter(col("pv_uv") > 0),
        "item_conversion_rate"
      )

      val categoryTopN = categoryEfficiency
        .withColumn("rank_no", row_number().over(Window.orderBy(col("pv_cnt").desc, col("buy_cnt").desc, col("category_id").asc)))
        .filter(col("rank_no") <= 20)
        .select("rank_no", "category_id", "pv_cnt", "pv_uv", "fav_cnt", "cart_cnt", "buy_cnt", "buy_uv", "category_conversion_rate")

      val itemTopN = itemEfficiency
        .withColumn("rank_no", row_number().over(Window.orderBy(col("pv_cnt").desc, col("buy_cnt").desc, col("item_id").asc)))
        .filter(col("rank_no") <= 20)
        .select("rank_no", "item_id", "category_id", "pv_cnt", "pv_uv", "intent_cnt", "buy_cnt", "buy_uv", "item_conversion_rate")

      val categoryConversionRank = categoryEfficiency
        .filter(col("pv_uv") >= 100)
        .withColumn("rank_no", row_number().over(Window.orderBy(col("category_conversion_rate").desc, col("buy_uv").desc, col("pv_cnt").desc)))
        .filter(col("rank_no") <= 20)
        .select("rank_no", "category_id", "pv_cnt", "pv_uv", "fav_cnt", "cart_cnt", "buy_cnt", "buy_uv", "category_conversion_rate")

      val categoryLowConversion = categoryEfficiency
        .withColumn("avg_pv", avg(col("pv_cnt")).over(Window.partitionBy()))
        .filter(col("pv_cnt") >= col("avg_pv"))
        .drop("avg_pv")
        .withColumn("rank_no", row_number().over(Window.orderBy(col("category_conversion_rate").asc, col("pv_cnt").desc)))
        .filter(col("rank_no") <= 20)
        .select("rank_no", "category_id", "pv_cnt", "pv_uv", "fav_cnt", "cart_cnt", "buy_cnt", "buy_uv", "category_conversion_rate")

      val longTailWindow = Window.orderBy(col("pv_cnt").desc, col("buy_cnt").desc, col("item_id").asc)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
      val allItemsWindow = Window.partitionBy()
      val itemLongTail = itemEfficiency
        .withColumn("rank_no", row_number().over(Window.orderBy(col("pv_cnt").desc, col("buy_cnt").desc, col("item_id").asc)))
        .withColumn("cumulative_pv", sum(col("pv_cnt")).over(longTailWindow).cast("long"))
        .withColumn("total_pv", sum(col("pv_cnt")).over(allItemsWindow).cast("long"))
        .withColumn("cumulative_pv_rate", round(col("cumulative_pv").cast("double") / col("total_pv").cast("double"), 4))
        .withColumn(
          "tail_segment",
          when(col("cumulative_pv_rate") <= 0.8, lit("head"))
            .when(col("cumulative_pv_rate") <= 0.95, lit("middle"))
            .otherwise(lit("long_tail"))
        )
        .select(
          "rank_no",
          "item_id",
          "category_id",
          "pv_cnt",
          "pv_uv",
          "buy_cnt",
          "buy_uv",
          "item_conversion_rate",
          "cumulative_pv",
          "total_pv",
          "cumulative_pv_rate",
          "tail_segment"
        )

      val resultTables = Seq(
        ResultTable(
          "lb_category_efficiency",
          s"$outputBasePath/lb_category_efficiency",
          categoryEfficiency,
          """
            |category_id BIGINT,
            |pv_cnt BIGINT,
            |pv_uv BIGINT,
            |fav_cnt BIGINT,
            |cart_cnt BIGINT,
            |buy_cnt BIGINT,
            |buy_uv BIGINT,
            |category_conversion_rate DOUBLE
            |""".stripMargin),
        ResultTable(
          "lb_item_efficiency",
          s"$outputBasePath/lb_item_efficiency",
          itemEfficiency,
          """
            |item_id BIGINT,
            |category_id BIGINT,
            |pv_cnt BIGINT,
            |pv_uv BIGINT,
            |fav_cnt BIGINT,
            |cart_cnt BIGINT,
            |intent_cnt BIGINT,
            |buy_cnt BIGINT,
            |buy_uv BIGINT,
            |item_conversion_rate DOUBLE
            |""".stripMargin),
        ResultTable(
          "lb_category_topn",
          s"$outputBasePath/lb_category_topn",
          categoryTopN,
          categoryRankSchema),
        ResultTable(
          "lb_item_topn",
          s"$outputBasePath/lb_item_topn",
          itemTopN,
          """
            |rank_no INT,
            |item_id BIGINT,
            |category_id BIGINT,
            |pv_cnt BIGINT,
            |pv_uv BIGINT,
            |intent_cnt BIGINT,
            |buy_cnt BIGINT,
            |buy_uv BIGINT,
            |item_conversion_rate DOUBLE
            |""".stripMargin),
        ResultTable(
          "lb_category_conversion_rank",
          s"$outputBasePath/lb_category_conversion_rank",
          categoryConversionRank,
          categoryRankSchema),
        ResultTable(
          "lb_category_low_conversion",
          s"$outputBasePath/lb_category_low_conversion",
          categoryLowConversion,
          categoryRankSchema),
        ResultTable(
          "lb_item_long_tail",
          s"$outputBasePath/lb_item_long_tail",
          itemLongTail,
          """
            |rank_no INT,
            |item_id BIGINT,
            |category_id BIGINT,
            |pv_cnt BIGINT,
            |pv_uv BIGINT,
            |buy_cnt BIGINT,
            |buy_uv BIGINT,
            |item_conversion_rate DOUBLE,
            |cumulative_pv BIGINT,
            |total_pv BIGINT,
            |cumulative_pv_rate DOUBLE,
            |tail_segment STRING
            |""".stripMargin)
      )

      resultTables.foreach(createExternalParquetTable(spark, database, _))
      resultTables.foreach(writeParquetResult)

      println("[SUCCESS] Category and item efficiency analysis completed")
      categoryTopN.show(20, truncate = false)
      itemTopN.show(20, truncate = false)
      categoryConversionRank.show(20, truncate = false)
      categoryLowConversion.show(20, truncate = false)
      itemLongTail.show(20, truncate = false)
    } finally {
      spark.stop()
    }
  }

  private val categoryRankSchema: String =
    """
      |rank_no INT,
      |category_id BIGINT,
      |pv_cnt BIGINT,
      |pv_uv BIGINT,
      |fav_cnt BIGINT,
      |cart_cnt BIGINT,
      |buy_cnt BIGINT,
      |buy_uv BIGINT,
      |category_conversion_rate DOUBLE
      |""".stripMargin

  private def addConversionRate(df: DataFrame, rateColumn: String): DataFrame = {
    df.withColumn(
      rateColumn,
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
