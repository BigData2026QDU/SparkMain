package org.example.analysis

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Scala Spark implementation for LuckyAnJun issue #15.
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
      .appName("LuckyAnJun-UserBehaviorFunnelJob")
      .enableHiveSupport()
      .getOrCreate()

    try {
      spark.sql(s"USE $database")

      val dwd = spark.table(sourceTable)
        .filter(col("user_id").isNotNull)
        .filter(col("event_date").isNotNull && col("event_date") =!= "event_date")
        .filter(col("behavior_type").isin("pv", "fav", "cart", "buy"))

      val userFlags = dwd
        .groupBy("user_id")
        .agg(
          max(when(col("behavior_type") === "pv", 1).otherwise(0)).as("has_pv"),
          max(when(col("behavior_type").isin("fav", "cart"), 1).otherwise(0)).as("has_intent"),
          max(when(col("behavior_type") === "buy", 1).otherwise(0)).as("has_buy")
        )

      val overall = addFunnelRates(aggregateFlags(userFlags))
      writeResultTable(spark, overall, database, "lb_funnel_overall", s"$outputBasePath/lb_funnel_overall")

      val dailyFlags = dwd
        .groupBy("event_date", "user_id")
        .agg(
          max(when(col("behavior_type") === "pv", 1).otherwise(0)).as("has_pv"),
          max(when(col("behavior_type").isin("fav", "cart"), 1).otherwise(0)).as("has_intent"),
          max(when(col("behavior_type") === "buy", 1).otherwise(0)).as("has_buy")
        )

      val dailyCounts = dailyFlags
        .groupBy("event_date")
        .agg(
          sum(when(col("has_pv") === 1, 1).otherwise(0)).cast("long").as("pv_users"),
          sum(when(col("has_intent") === 1, 1).otherwise(0)).cast("long").as("intent_users"),
          sum(when(col("has_buy") === 1, 1).otherwise(0)).cast("long").as("buy_users"),
          sum(when(col("has_pv") === 1 && col("has_intent") === 1, 1).otherwise(0)).cast("long").as("pv_to_intent_users"),
          sum(when(col("has_intent") === 1 && col("has_buy") === 1, 1).otherwise(0)).cast("long").as("intent_to_buy_users"),
          sum(when(col("has_pv") === 1 && col("has_buy") === 1, 1).otherwise(0)).cast("long").as("pv_to_buy_users"),
          sum(when(col("has_pv") === 1 && col("has_intent") === 0, 1).otherwise(0)).cast("long").as("pv_loss_users"),
          sum(when(col("has_intent") === 1 && col("has_buy") === 0, 1).otherwise(0)).cast("long").as("intent_loss_users")
        )

      val daily = addFunnelRates(dailyCounts)
      writeResultTable(spark, daily, database, "lb_funnel_daily", s"$outputBasePath/lb_funnel_daily")

      println("[SUCCESS] Funnel analysis completed")
      overall.show(truncate = false)
      daily.orderBy("event_date").show(20, truncate = false)
    } finally {
      spark.stop()
    }
  }

  private def aggregateFlags(flags: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    flags.agg(
      sum(when(col("has_pv") === 1, 1).otherwise(0)).cast("long").as("pv_users"),
      sum(when(col("has_intent") === 1, 1).otherwise(0)).cast("long").as("intent_users"),
      sum(when(col("has_buy") === 1, 1).otherwise(0)).cast("long").as("buy_users"),
      sum(when(col("has_pv") === 1 && col("has_intent") === 1, 1).otherwise(0)).cast("long").as("pv_to_intent_users"),
      sum(when(col("has_intent") === 1 && col("has_buy") === 1, 1).otherwise(0)).cast("long").as("intent_to_buy_users"),
      sum(when(col("has_pv") === 1 && col("has_buy") === 1, 1).otherwise(0)).cast("long").as("pv_to_buy_users"),
      sum(when(col("has_pv") === 1 && col("has_intent") === 0, 1).otherwise(0)).cast("long").as("pv_loss_users"),
      sum(when(col("has_intent") === 1 && col("has_buy") === 0, 1).otherwise(0)).cast("long").as("intent_loss_users")
    )
  }

  private def addFunnelRates(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    df
      .withColumn("pv_to_intent_rate", rate("pv_to_intent_users", "pv_users"))
      .withColumn("intent_to_buy_rate", rate("intent_to_buy_users", "intent_users"))
      .withColumn("pv_to_buy_rate", rate("pv_to_buy_users", "pv_users"))
      .withColumn("pv_loss_rate", rate("pv_loss_users", "pv_users"))
      .withColumn("intent_loss_rate", rate("intent_loss_users", "intent_users"))
  }

  private def rate(numerator: String, denominator: String) = {
    round(
      when(col(denominator) === 0, lit(0.0))
        .otherwise(col(numerator).cast("double") / col(denominator).cast("double")),
      4
    )
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
