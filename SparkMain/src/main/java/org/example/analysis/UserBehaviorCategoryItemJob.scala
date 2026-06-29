package org.example.analysis

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * LuckyAnJun issue #17: popular category Top20.
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
      .appName("LuckyAnJun-CategoryTop20")
      .enableHiveSupport()
      .getOrCreate()

    try {
      spark.sql(s"USE $database")

      val events = spark.table(sourceTable)
        .select(
          col("user_id").cast("long").as("user_id"),
          col("category_id").cast("long").as("category_id"),
          lower(col("behavior_type")).as("behavior_type"))
        .filter(col("user_id").isNotNull && col("user_id") > 0)
        .filter(col("category_id").isNotNull && col("category_id") > 0)
        .filter(col("behavior_type").isin("pv", "fav", "cart", "buy"))

      val categoryTopN = events
        .groupBy("category_id")
        .agg(
          count(when(col("behavior_type") === "pv", 1)).cast("long").as("pv_cnt"),
          countDistinct(when(col("behavior_type") === "pv", col("user_id"))).cast("long").as("pv_uv"),
          count(when(col("behavior_type") === "fav", 1)).cast("long").as("fav_cnt"),
          count(when(col("behavior_type") === "cart", 1)).cast("long").as("cart_cnt"),
          count(when(col("behavior_type") === "buy", 1)).cast("long").as("buy_cnt"),
          countDistinct(when(col("behavior_type") === "buy", col("user_id"))).cast("long").as("buy_uv"))
        .filter(col("pv_uv") > 0)
        .withColumn(
          "category_conversion_rate",
          round(col("buy_uv").cast("double") / col("pv_uv").cast("double"), 4))
        .withColumn(
          "rank_no",
          row_number().over(
            Window.orderBy(col("pv_cnt").desc, col("buy_cnt").desc, col("category_id").asc)))
        .filter(col("rank_no") <= 20)
        .select(
          "rank_no",
          "category_id",
          "pv_cnt",
          "pv_uv",
          "fav_cnt",
          "cart_cnt",
          "buy_cnt",
          "buy_uv",
          "category_conversion_rate")

      writeResultTable(
        spark,
        categoryTopN,
        database,
        "lb_category_topn",
        s"$outputBasePath/lb_category_topn")

      println("[SUCCESS] Category Top20 completed")
      categoryTopN.orderBy("rank_no").show(20, truncate = false)
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
