package org.example.analysis

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Scala Spark implementation for LuckyAnJun issue #14.
 *
 * Usage:
 *   spark-submit --class org.example.analysis.UserBehaviorCleanJob <jar> \
 *     <inputCsv> <outputPath> [database]
 */
object UserBehaviorCleanJob {
  private val ValidBehaviors = Seq("pv", "buy", "cart", "fav")

  def main(args: Array[String]): Unit = {
    val inputCsv = args.lift(0).getOrElse("dataset/UserBehavior.csv")
    val outputPath = args.lift(1).getOrElse("/user/hive/bigdata_ana/user_behavior")
    val database = args.lift(2).getOrElse("bigdata_ana")

    val spark = SparkSession.builder()
      .appName("LuckyAnJun-UserBehaviorCleanJob")
      .config("spark.sql.session.timeZone", "Asia/Shanghai")
      .enableHiveSupport()
      .getOrCreate()

    try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
      spark.sql(s"USE $database")

      val rawSchema = StructType(Seq(
        StructField("user_id_raw", StringType, nullable = true),
        StructField("item_id_raw", StringType, nullable = true),
        StructField("category_id_raw", StringType, nullable = true),
        StructField("behavior_type_raw", StringType, nullable = true),
        StructField("timestamp_raw", StringType, nullable = true)
      ))

      val raw = spark.read
        .option("header", "false")
        .schema(rawSchema)
        .csv(inputCsv)

      val typed = raw.select(
        col("user_id_raw").cast(LongType).as("user_id"),
        col("item_id_raw").cast(LongType).as("item_id"),
        col("category_id_raw").cast(LongType).as("category_id"),
        lower(trim(col("behavior_type_raw"))).as("behavior_type"),
        col("timestamp_raw").cast(LongType).as("timestamp")
      )

      val eventTime = from_unixtime(col("timestamp")).cast(TimestampType)
      val cleaned = typed
        .filter(col("user_id") > 0)
        .filter(col("item_id") > 0)
        .filter(col("category_id") > 0)
        .filter(col("timestamp") > 0)
        .filter(col("behavior_type").isin(ValidBehaviors: _*))
        .withColumn("event_time", eventTime)
        .withColumn("event_date", to_date(col("event_time")).cast(StringType))
        .withColumn("event_hour", hour(col("event_time")))
        .withColumn("weekday", ((dayofweek(col("event_time")) + lit(5)) % lit(7)) + lit(1))
        .filter(col("event_date").between("2017-11-25", "2017-12-03"))
        .select(
          col("user_id"),
          col("item_id"),
          col("category_id"),
          col("behavior_type"),
          col("timestamp"),
          col("event_time"),
          col("event_date"),
          col("event_hour"),
          col("weekday")
        )

      cleaned.write
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(outputPath)

      spark.sql("DROP TABLE IF EXISTS user_behavior")
      spark.sql(
        s"""
           |CREATE EXTERNAL TABLE user_behavior (
           |    user_id BIGINT,
           |    item_id BIGINT,
           |    category_id BIGINT,
           |    behavior_type STRING,
           |    `timestamp` BIGINT,
           |    event_time TIMESTAMP,
           |    event_date STRING,
           |    event_hour INT,
           |    weekday INT
           |)
           |ROW FORMAT DELIMITED
           |FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE
           |LOCATION '$outputPath'
           |TBLPROPERTIES ("skip.header.line.count"="1")
           |""".stripMargin)

      spark.sql("DROP TABLE IF EXISTS dwd_user_behavior_clean")
      spark.sql(
        s"""
           |CREATE EXTERNAL TABLE dwd_user_behavior_clean (
           |    user_id BIGINT,
           |    item_id BIGINT,
           |    category_id BIGINT,
           |    behavior_type STRING,
           |    `timestamp` BIGINT,
           |    event_time TIMESTAMP,
           |    event_date STRING,
           |    event_hour INT,
           |    weekday INT
           |)
           |ROW FORMAT DELIMITED
           |FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE
           |LOCATION '$outputPath'
           |TBLPROPERTIES ("skip.header.line.count"="1")
           |""".stripMargin)

      val count = cleaned.count()
      println(s"[SUCCESS] Cleaned UserBehavior rows: $count")
      println(s"[SUCCESS] Output path: $outputPath")
    } finally {
      spark.stop()
    }
  }
}
