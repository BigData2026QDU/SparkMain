package org.example.analysis

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * LuckyAnJun issue #15: strict user-item funnel.
 *
 * Usage:
 *   spark-submit --class org.example.analysis.UserBehaviorFunnelJob <jar> \
 *     [database] [sourceTable] [outputBasePath]
 */
object UserBehaviorFunnelJob {
  def main(args: Array[String]): Unit = {
    val database = args.lift(0).getOrElse("bigdata_ana")
    val sourceTable = args.lift(1).getOrElse("dwd_user_behavior_clean")
    val outputBasePath = args.lift(2).getOrElse("/user/hive/bigdata_ana")

    val spark = SparkSession.builder()
      .appName("LuckyAnJun-StrictItemFunnel")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    try {
      spark.sql(s"USE $database")

      val events = spark.table(sourceTable)
        .select(
          col("user_id").cast("long").as("user_id"),
          col("item_id").cast("long").as("item_id"),
          lower(col("behavior_type")).as("behavior_type"),
          col("timestamp").cast("long").as("event_ts"))
        .filter(col("user_id").isNotNull && col("user_id") > 0)
        .filter(col("item_id").isNotNull && col("item_id") > 0)
        .filter(col("event_ts").isNotNull)
        .filter(col("behavior_type").isin("pv", "fav", "cart", "buy"))

      val firstPv = events
        .groupBy("user_id", "item_id")
        .agg(min(when(col("behavior_type") === "pv", col("event_ts"))).as("first_pv_ts"))
        .filter(col("first_pv_ts").isNotNull)

      val firstIntentAfterPv = events
        .join(firstPv, Seq("user_id", "item_id"))
        .filter(col("event_ts") >= col("first_pv_ts"))
        .groupBy("user_id", "item_id")
        .agg(
          min(when(col("behavior_type").isin("fav", "cart"), col("event_ts")))
            .as("first_intent_after_pv_ts"))
        .filter(col("first_intent_after_pv_ts").isNotNull)

      val buyAfterIntent = events
        .filter(col("behavior_type") === "buy")
        .join(firstIntentAfterPv, Seq("user_id", "item_id"))
        .filter(col("event_ts") >= col("first_intent_after_pv_ts"))
        .select("user_id", "item_id")
        .distinct()

      val viewedPairs = firstPv.count()
      val intentPairs = firstIntentAfterPv.count()
      val buyAfterIntentPairs = buyAfterIntent.count()

      val itemPathStage = Seq(
        (1, "浏览商品", viewedPairs, 1.0),
        (2, "浏览后收藏/加购同商品", intentPairs, safeRate(intentPairs, viewedPairs)),
        (3, "意向后购买同商品", buyAfterIntentPairs, safeRate(buyAfterIntentPairs, viewedPairs))
      ).toDF("stage_order", "stage_name", "pair_cnt", "conversion_rate")

      writeResultTable(
        spark,
        itemPathStage,
        database,
        "lb_funnel_item_path_stage",
        s"$outputBasePath/lb_funnel_item_path_stage")

      println("[SUCCESS] Strict item funnel completed")
      itemPathStage.orderBy("stage_order").show(3, truncate = false)
    } finally {
      spark.stop()
    }
  }

  private def safeRate(numerator: Long, denominator: Long): Double = {
    if (denominator == 0L) 0.0
    else BigDecimal(numerator.toDouble / denominator.toDouble)
      .setScale(6, BigDecimal.RoundingMode.HALF_UP)
      .toDouble
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
