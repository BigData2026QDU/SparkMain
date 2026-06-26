package org.example.analysis

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Scala Spark implementation for LuckyAnJun issue #18.
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
      .appName("LuckyAnJun-UserBehaviorSegmentRetentionJob")
      .enableHiveSupport()
      .getOrCreate()

    try {
      spark.sql(s"USE $database")

      val dwd = spark.table(sourceTable)
        .select(
          col("user_id").cast("long").as("user_id"),
          col("item_id").cast("long").as("item_id"),
          lower(col("behavior_type")).as("behavior_type"),
          col("event_date").cast("string").as("event_date")
        )
        .filter(col("user_id").isNotNull && col("user_id") > 0)
        .filter(col("item_id").isNotNull && col("item_id") > 0)
        .filter(col("event_date").isNotNull && col("event_date") =!= "event_date")
        .filter(col("behavior_type").isin("pv", "fav", "cart", "buy"))

      val userFeatures = dwd.groupBy("user_id")
        .agg(
          min(col("event_date")).as("cohort_date"),
          max(col("event_date")).as("last_event_date"),
          count(lit(1)).cast("long").as("behavior_cnt"),
          countDistinct(col("event_date")).cast("long").as("active_days"),
          countDistinct(col("item_id")).cast("long").as("distinct_item_cnt"),
          count(when(col("behavior_type") === "pv", 1)).cast("long").as("pv_cnt"),
          count(when(col("behavior_type") === "fav", 1)).cast("long").as("fav_cnt"),
          count(when(col("behavior_type") === "cart", 1)).cast("long").as("cart_cnt"),
          count(when(col("behavior_type").isin("fav", "cart"), 1)).cast("long").as("intent_cnt"),
          count(when(col("behavior_type") === "buy", 1)).cast("long").as("buy_cnt"),
          countDistinct(when(col("behavior_type") === "buy", col("event_date"))).cast("long").as("buy_days")
        )

      val ranked = userFeatures
        .withColumn("active_days_percentile", cume_dist().over(Window.orderBy(col("active_days"))))
        .withColumn("behavior_cnt_percentile", cume_dist().over(Window.orderBy(col("behavior_cnt"))))

      val userSegments = ranked
        .withColumn(
          "is_high_active",
          when(col("active_days_percentile") >= 0.8 && col("behavior_cnt_percentile") >= 0.8, lit(1)).otherwise(lit(0))
        )
        .withColumn(
          "user_segment",
          when(col("pv_cnt") > 0 && col("intent_cnt") === 0 && col("buy_cnt") === 0, lit("browse_only"))
            .when(col("intent_cnt") > 0 && col("buy_cnt") === 0, lit("intent_only"))
            .when(col("buy_days") === 1, lit("first_buy"))
            .when(col("buy_days") >= 2, lit("short_repurchase"))
            .otherwise(lit("other"))
        )
        .withColumn("is_short_repurchase", when(col("buy_days") >= 2, lit(1)).otherwise(lit(0)))
        .select(
          "user_id",
          "cohort_date",
          "last_event_date",
          "active_days",
          "behavior_cnt",
          "distinct_item_cnt",
          "pv_cnt",
          "fav_cnt",
          "cart_cnt",
          "intent_cnt",
          "buy_cnt",
          "buy_days",
          "is_high_active",
          "user_segment",
          "is_short_repurchase"
        )

      val totalUsers = userSegments.agg(count(lit(1)).cast("long").as("total_users"))

      val lifecycleSummary = userSegments.groupBy("user_segment")
        .agg(summaryAggs.head, summaryAggs.tail: _*)
        .crossJoin(totalUsers)
        .withColumn("segment_type", lit("lifecycle"))
        .withColumn("user_rate", rate("user_cnt", "total_users"))
        .select(summaryColumns: _*)

      val activitySummary = userSegments
        .agg(
          sum(when(col("is_high_active") === 1, 1).otherwise(0)).cast("long").as("user_cnt"),
          round(avg(when(col("is_high_active") === 1, col("active_days"))), 2).as("avg_active_days"),
          round(avg(when(col("is_high_active") === 1, col("behavior_cnt"))), 2).as("avg_behavior_cnt"),
          round(avg(when(col("is_high_active") === 1, col("distinct_item_cnt"))), 2).as("avg_distinct_item_cnt"),
          round(avg(when(col("is_high_active") === 1, col("pv_cnt"))), 2).as("avg_pv_cnt"),
          round(avg(when(col("is_high_active") === 1, col("intent_cnt"))), 2).as("avg_intent_cnt"),
          round(avg(when(col("is_high_active") === 1, col("buy_cnt"))), 2).as("avg_buy_cnt")
        )
        .crossJoin(totalUsers)
        .withColumn("segment_type", lit("activity"))
        .withColumn("user_segment", lit("high_active"))
        .withColumn("user_rate", rate("user_cnt", "total_users"))
        .select(summaryColumns: _*)

      val segmentSummary = lifecycleSummary.unionByName(activitySummary)

      val activeDayDistribution = userSegments.groupBy("active_days")
        .agg(
          count(lit(1)).cast("long").as("user_cnt"),
          round(avg(col("behavior_cnt")), 2).as("avg_behavior_cnt"),
          round(avg(col("distinct_item_cnt")), 2).as("avg_distinct_item_cnt"),
          round(avg(col("buy_cnt")), 2).as("avg_buy_cnt")
        )
        .crossJoin(totalUsers)
        .withColumn("user_rate", rate("user_cnt", "total_users"))
        .select(
          col("active_days"),
          col("user_cnt"),
          col("user_rate"),
          col("avg_behavior_cnt"),
          col("avg_distinct_item_cnt"),
          col("avg_buy_cnt")
        )

      val activeEvents = dwd.select("user_id", "event_date").distinct()
      val retentionJoined = userFeatures
        .select("user_id", "cohort_date")
        .join(activeEvents, Seq("user_id"), "left")
        .withColumn("retention_day", datediff(to_date(col("event_date")), to_date(col("cohort_date"))))

      val userRetention = retentionJoined.groupBy("cohort_date")
        .agg(
          countDistinct(col("user_id")).cast("long").as("cohort_users"),
          countDistinct(when(col("retention_day") === 1, col("user_id"))).cast("long").as("day1_retained_users"),
          countDistinct(when(col("retention_day") === 3, col("user_id"))).cast("long").as("day3_retained_users"),
          countDistinct(when(col("retention_day") === 7, col("user_id"))).cast("long").as("day7_retained_users")
        )
        .withColumn("day1_retention_rate", rate("day1_retained_users", "cohort_users"))
        .withColumn("day3_retention_rate", rate("day3_retained_users", "cohort_users"))
        .withColumn("day7_retention_rate", rate("day7_retained_users", "cohort_users"))
        .select(
          "cohort_date",
          "cohort_users",
          "day1_retained_users",
          "day1_retention_rate",
          "day3_retained_users",
          "day3_retention_rate",
          "day7_retained_users",
          "day7_retention_rate"
        )

      val retentionHeatmap = Seq(
        userRetention.select(
          col("cohort_date"),
          lit(1).as("retention_day"),
          col("cohort_users"),
          col("day1_retained_users").as("retained_users"),
          col("day1_retention_rate").as("retention_rate")),
        userRetention.select(
          col("cohort_date"),
          lit(3).as("retention_day"),
          col("cohort_users"),
          col("day3_retained_users").as("retained_users"),
          col("day3_retention_rate").as("retention_rate")),
        userRetention.select(
          col("cohort_date"),
          lit(7).as("retention_day"),
          col("cohort_users"),
          col("day7_retained_users").as("retained_users"),
          col("day7_retention_rate").as("retention_rate"))
      ).reduce(_.unionByName(_))

      val repurchaseDepth = userSegments
        .withColumn(
          "repurchase_group",
          when(col("is_short_repurchase") === 1, lit("short_repurchase")).otherwise(lit("non_repurchase"))
        )
        .groupBy("repurchase_group")
        .agg(
          count(lit(1)).cast("long").as("user_cnt"),
          round(avg(col("active_days")), 2).as("avg_active_days"),
          round(avg(col("behavior_cnt")), 2).as("avg_behavior_cnt"),
          round(avg(col("distinct_item_cnt")), 2).as("avg_distinct_item_cnt"),
          round(avg(col("pv_cnt")), 2).as("avg_pv_cnt"),
          round(avg(col("fav_cnt")), 2).as("avg_fav_cnt"),
          round(avg(col("cart_cnt")), 2).as("avg_cart_cnt"),
          round(avg(col("intent_cnt")), 2).as("avg_intent_cnt"),
          round(avg(col("buy_cnt")), 2).as("avg_buy_cnt"),
          round(avg(col("buy_days")), 2).as("avg_buy_days")
        )

      val resultTables = Seq(
        ResultTable("lb_user_features", s"$outputBasePath/lb_user_features", userFeatures, userFeaturesSchema),
        ResultTable("lb_user_segments", s"$outputBasePath/lb_user_segments", userSegments, userSegmentsSchema),
        ResultTable("lb_user_segment_summary", s"$outputBasePath/lb_user_segment_summary", segmentSummary, segmentSummarySchema),
        ResultTable("lb_user_active_day_distribution", s"$outputBasePath/lb_user_active_day_distribution", activeDayDistribution, activeDayDistributionSchema),
        ResultTable("lb_user_retention", s"$outputBasePath/lb_user_retention", userRetention, userRetentionSchema),
        ResultTable("lb_user_retention_heatmap", s"$outputBasePath/lb_user_retention_heatmap", retentionHeatmap, retentionHeatmapSchema),
        ResultTable("lb_repurchase_behavior_depth", s"$outputBasePath/lb_repurchase_behavior_depth", repurchaseDepth, repurchaseDepthSchema)
      )

      resultTables.foreach(createExternalParquetTable(spark, database, _))
      resultTables.foreach(writeParquetResult)

      println("[SUCCESS] User segment and retention analysis completed")
      resultTables.foreach(result => println(s"[OUTPUT] $database.${result.tableName} -> ${result.outputPath}"))
    } finally {
      spark.stop()
    }
  }

  private val summaryAggs = Seq(
    count(lit(1)).cast("long").as("user_cnt"),
    round(avg(col("active_days")), 2).as("avg_active_days"),
    round(avg(col("behavior_cnt")), 2).as("avg_behavior_cnt"),
    round(avg(col("distinct_item_cnt")), 2).as("avg_distinct_item_cnt"),
    round(avg(col("pv_cnt")), 2).as("avg_pv_cnt"),
    round(avg(col("intent_cnt")), 2).as("avg_intent_cnt"),
    round(avg(col("buy_cnt")), 2).as("avg_buy_cnt")
  )

  private val summaryColumns = Seq(
    col("segment_type"),
    col("user_segment"),
    col("user_cnt"),
    col("user_rate"),
    col("avg_active_days"),
    col("avg_behavior_cnt"),
    col("avg_distinct_item_cnt"),
    col("avg_pv_cnt"),
    col("avg_intent_cnt"),
    col("avg_buy_cnt")
  )

  private val userFeaturesSchema: String =
    """
      |user_id BIGINT,
      |cohort_date STRING,
      |last_event_date STRING,
      |behavior_cnt BIGINT,
      |active_days BIGINT,
      |distinct_item_cnt BIGINT,
      |pv_cnt BIGINT,
      |fav_cnt BIGINT,
      |cart_cnt BIGINT,
      |intent_cnt BIGINT,
      |buy_cnt BIGINT,
      |buy_days BIGINT
      |""".stripMargin

  private val userSegmentsSchema: String =
    """
      |user_id BIGINT,
      |cohort_date STRING,
      |last_event_date STRING,
      |active_days BIGINT,
      |behavior_cnt BIGINT,
      |distinct_item_cnt BIGINT,
      |pv_cnt BIGINT,
      |fav_cnt BIGINT,
      |cart_cnt BIGINT,
      |intent_cnt BIGINT,
      |buy_cnt BIGINT,
      |buy_days BIGINT,
      |is_high_active INT,
      |user_segment STRING,
      |is_short_repurchase INT
      |""".stripMargin

  private val segmentSummarySchema: String =
    """
      |segment_type STRING,
      |user_segment STRING,
      |user_cnt BIGINT,
      |user_rate DOUBLE,
      |avg_active_days DOUBLE,
      |avg_behavior_cnt DOUBLE,
      |avg_distinct_item_cnt DOUBLE,
      |avg_pv_cnt DOUBLE,
      |avg_intent_cnt DOUBLE,
      |avg_buy_cnt DOUBLE
      |""".stripMargin

  private val activeDayDistributionSchema: String =
    """
      |active_days BIGINT,
      |user_cnt BIGINT,
      |user_rate DOUBLE,
      |avg_behavior_cnt DOUBLE,
      |avg_distinct_item_cnt DOUBLE,
      |avg_buy_cnt DOUBLE
      |""".stripMargin

  private val userRetentionSchema: String =
    """
      |cohort_date STRING,
      |cohort_users BIGINT,
      |day1_retained_users BIGINT,
      |day1_retention_rate DOUBLE,
      |day3_retained_users BIGINT,
      |day3_retention_rate DOUBLE,
      |day7_retained_users BIGINT,
      |day7_retention_rate DOUBLE
      |""".stripMargin

  private val retentionHeatmapSchema: String =
    """
      |cohort_date STRING,
      |retention_day INT,
      |cohort_users BIGINT,
      |retained_users BIGINT,
      |retention_rate DOUBLE
      |""".stripMargin

  private val repurchaseDepthSchema: String =
    """
      |repurchase_group STRING,
      |user_cnt BIGINT,
      |avg_active_days DOUBLE,
      |avg_behavior_cnt DOUBLE,
      |avg_distinct_item_cnt DOUBLE,
      |avg_pv_cnt DOUBLE,
      |avg_fav_cnt DOUBLE,
      |avg_cart_cnt DOUBLE,
      |avg_intent_cnt DOUBLE,
      |avg_buy_cnt DOUBLE,
      |avg_buy_days DOUBLE
      |""".stripMargin

  private def rate(numerator: String, denominator: String) = {
    round(
      when(col(denominator) === 0, lit(0.0))
        .otherwise(col(numerator).cast("double") / col(denominator).cast("double")),
      4
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
