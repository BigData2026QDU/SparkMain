package org.bigdata.streaming

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.bigdata.utils.MySQLExporter

final case class PersonalRealtimeConfig(
    source: String,
    inputPath: String,
    kafkaBootstrapServers: String,
    kafkaTopic: String,
    kafkaStartingOffsets: String,
    checkpointPath: String,
    outputBasePath: String,
    windowDuration: String,
    triggerOnce: Boolean,
    mysqlEnabled: Boolean,
    mysqlJdbcUrl: String,
    mysqlUser: String,
    mysqlPassword: String,
    metricsTable: String,
    topMoviesTable: String,
    alertsTable: String,
    alertMinRatingCount: Long,
    alertLowAvgRating: Double,
    alertHighAvgRating: Double)

object PersonalRealtimeConfig {
  def fromEnvironment(env: Map[String, String] = sys.env): PersonalRealtimeConfig = {
    def value(name: String, default: String): String =
      env.get(name).map(_.trim).filter(_.nonEmpty).getOrElse(default)

    def flag(name: String, default: Boolean): Boolean =
      env.get(name).map(_.trim.toLowerCase) match {
        case Some("true") | Some("1") | Some("yes") => true
        case Some("false") | Some("0") | Some("no") => false
        case Some(other) =>
          throw new IllegalArgumentException(
            s"$name must be true or false, but was '$other'")
        case None => default
      }

    def longValue(name: String, default: Long): Long =
      value(name, default.toString).toLong

    def doubleValue(name: String, default: Double): Double =
      value(name, default.toString).toDouble

    PersonalRealtimeConfig(
      source = value("REALTIME_SOURCE", "file").toLowerCase,
      inputPath = value("REALTIME_INPUT_PATH", "dataset_test/realtime_ratings"),
      kafkaBootstrapServers =
        value("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
      kafkaTopic = value("KAFKA_TOPIC", "ratings"),
      kafkaStartingOffsets = value("KAFKA_STARTING_OFFSETS", "latest"),
      checkpointPath =
        value("REALTIME_CHECKPOINT_PATH", "output/checkpoints/personal_realtime_ratings"),
      outputBasePath =
        value("REALTIME_OUTPUT_PATH", "output/personal_realtime"),
      windowDuration = value("REALTIME_WINDOW", "5 minutes"),
      triggerOnce = flag("REALTIME_TRIGGER_ONCE", default = true),
      mysqlEnabled = flag("MYSQL_ENABLED", default = false),
      mysqlJdbcUrl = value("MYSQL_JDBC_URL", ""),
      mysqlUser = value("MYSQL_USER", ""),
      mysqlPassword = value("MYSQL_PASSWORD", ""),
      metricsTable = value("MYSQL_REALTIME_METRICS_TABLE", "realtime_rating_metrics"),
      topMoviesTable = value("MYSQL_REALTIME_TOP_MOVIES_TABLE", "realtime_top_movies"),
      alertsTable = value("MYSQL_REALTIME_ALERTS_TABLE", "realtime_rating_alerts"),
      alertMinRatingCount = longValue("REALTIME_ALERT_MIN_COUNT", 2L),
      alertLowAvgRating = doubleValue("REALTIME_ALERT_LOW_AVG", 3.0),
      alertHighAvgRating = doubleValue("REALTIME_ALERT_HIGH_AVG", 4.5))
  }
}

/**
 * Personal realtime rating analysis.
 *
 * File mode replays CSV files as a streaming source for local and VM validation.
 * Kafka mode expects each message value to be a JSON rating record.
 */
object PersonalRealtimeRatings {
  private val RatingSchema = new StructType()
    .add("userId", DataTypes.IntegerType)
    .add("movieId", DataTypes.IntegerType)
    .add("rating", DataTypes.DoubleType)
    .add("timestamp", DataTypes.LongType)

  def main(args: Array[String]): Unit = {
    val config = PersonalRealtimeConfig.fromEnvironment()

    val spark = SparkSession.builder()
      .appName("PersonalRealtimeRatings")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config(
        "spark.sql.shuffle.partitions",
        sys.env.getOrElse("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
      .getOrCreate()

    val ratings = readRatings(spark, config)
      .filter(
        col("userId").isNotNull &&
          col("movieId").isNotNull &&
          col("rating").isNotNull &&
          col("timestamp").isNotNull)
      .withColumn("event_timestamp", to_timestamp(from_unixtime(col("timestamp"))))

    val writer = ratings.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        processBatch(batchDF, batchId, config)
      }
      .option("checkpointLocation", config.checkpointPath)
      .outputMode("append")

    val query =
      if (config.triggerOnce) writer.trigger(Trigger.Once()).start()
      else writer.trigger(Trigger.ProcessingTime("10 seconds")).start()

    println("Personal realtime rating analysis started")
    println(s"source=${config.source}")
    println(s"window=${config.windowDuration}")
    println(s"output=${config.outputBasePath}")
    println(if (config.mysqlEnabled) s"mysql=${config.mysqlJdbcUrl}" else "mysql=disabled")

    query.awaitTermination()
    spark.stop()
  }

  private def readRatings(spark: SparkSession, config: PersonalRealtimeConfig): DataFrame = {
    config.source match {
      case "file" =>
        spark.readStream
          .schema(RatingSchema)
          .option("header", "true")
          .csv(config.inputPath)

      case "kafka" =>
        val kafkaValues = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", config.kafkaBootstrapServers)
          .option("subscribe", config.kafkaTopic)
          .option("startingOffsets", config.kafkaStartingOffsets)
          .load()
          .selectExpr("CAST(value AS STRING) AS json")

        kafkaValues
          .select(from_json(col("json"), RatingSchema).as("data"))
          .select(
            col("data.userId").as("userId"),
            col("data.movieId").as("movieId"),
            col("data.rating").as("rating"),
            col("data.timestamp").as("timestamp"))

      case other =>
        throw new IllegalArgumentException(
          s"REALTIME_SOURCE must be file or kafka, but was '$other'")
    }
  }

  private def processBatch(
      batchDF: DataFrame,
      batchId: Long,
      config: PersonalRealtimeConfig): Unit = {
    if (batchDF.head(1).isEmpty) {
      println(s"batch=$batchId empty")
      return
    }

    val metrics = batchDF
      .groupBy(window(col("event_timestamp"), config.windowDuration))
      .agg(
        count("*").as("rating_count"),
        countDistinct("userId").as("active_user_count"),
        countDistinct("movieId").as("active_movie_count"),
        round(avg("rating"), 2).as("avg_rating"),
        max("rating").as("max_rating"),
        min("rating").as("min_rating"))
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("rating_count"),
        col("active_user_count"),
        col("active_movie_count"),
        col("avg_rating"),
        col("max_rating"),
        col("min_rating"))
      .withColumn("batch_id", lit(batchId))

    val topMovies = batchDF
      .groupBy("movieId")
      .agg(
        count("*").as("rating_count"),
        countDistinct("userId").as("active_user_count"),
        round(avg("rating"), 2).as("avg_rating"))
      .orderBy(desc("rating_count"), desc("avg_rating"))
      .limit(10)
      .withColumn("batch_id", lit(batchId))

    val alerts = metrics
      .filter(
        col("rating_count") >= lit(config.alertMinRatingCount) &&
          (col("avg_rating") <= lit(config.alertLowAvgRating) ||
            col("avg_rating") >= lit(config.alertHighAvgRating)))
      .withColumn(
        "alert_type",
        when(col("avg_rating") <= lit(config.alertLowAvgRating), lit("LOW_AVG_RATING"))
          .otherwise(lit("HIGH_AVG_RATING")))
      .withColumn(
        "alert_message",
        concat(
          lit("window average rating="),
          col("avg_rating").cast("string"),
          lit(", count="),
          col("rating_count").cast("string")))

    writeResult(metrics, s"${config.outputBasePath}/metrics", config.metricsTable, config)
    writeResult(topMovies, s"${config.outputBasePath}/top_movies", config.topMoviesTable, config)
    writeResult(alerts, s"${config.outputBasePath}/alerts", config.alertsTable, config)

    println(s"batch=$batchId metrics")
    metrics.show(20, truncate = false)
    println(s"batch=$batchId top movies")
    topMovies.show(10, truncate = false)
    println(s"batch=$batchId alerts")
    alerts.show(20, truncate = false)
  }

  private def writeResult(
      df: DataFrame,
      outputPath: String,
      mysqlTable: String,
      config: PersonalRealtimeConfig): Unit = {
    df.write.mode(SaveMode.Append).parquet(outputPath)

    if (config.mysqlEnabled) {
      if (config.mysqlJdbcUrl.isEmpty || config.mysqlUser.isEmpty) {
        throw new IllegalArgumentException(
          "MYSQL_ENABLED=true requires MYSQL_JDBC_URL and MYSQL_USER")
      }
      val props =
        MySQLExporter.createProperties(config.mysqlUser, config.mysqlPassword)
      MySQLExporter.exportToMySQL(
        df,
        mysqlTable,
        config.mysqlJdbcUrl,
        props,
        SaveMode.Append)
    }
  }
}
