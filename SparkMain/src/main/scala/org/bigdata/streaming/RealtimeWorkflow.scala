package org.bigdata.streaming

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.bigdata.utils.MySQLExporter

final case class RealtimeConfig(
    kafkaTopic: String,
    kafkaBootstrapServers: String,
    checkpointPath: String,
    outputPath: String,
    jdbcUrl: String,
    mysqlUser: String,
    mysqlPassword: String,
    mysqlTable: String,
    mysqlEnabled: Boolean)

object RealtimeConfig {
  private val ProductionJdbcUrl = "jdbc:mysql://localhost:3306/bigdata_ana"
  private val ProductionTable = "realtime_stats"

  def fromEnvironment(env: Map[String, String] = sys.env): RealtimeConfig = {
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

    val testMode = flag("SPARKMAIN_TEST_MODE", default = false)
    val jdbcUrl = value("MYSQL_JDBC_URL", ProductionJdbcUrl)
    val mysqlTable = value("MYSQL_TABLE", ProductionTable)
    val mysqlEnabled = flag("MYSQL_ENABLED", default = true)

    if (testMode && mysqlEnabled &&
        (jdbcUrl == ProductionJdbcUrl || mysqlTable == ProductionTable)) {
      throw new IllegalArgumentException(
        "test mode cannot write to production MySQL URL or table")
    }

    RealtimeConfig(
      kafkaTopic = value("KAFKA_TOPIC", "ratings"),
      kafkaBootstrapServers =
        value("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
      checkpointPath = value(
        "STREAMING_CHECKPOINT_PATH",
        "/tmp/spark/checkpoints/realtime_workflow"),
      outputPath = value("STREAMING_OUTPUT_PATH", "output/realtime_stats"),
      jdbcUrl = jdbcUrl,
      mysqlUser = value("MYSQL_USER", "root"),
      mysqlPassword = env.getOrElse("MYSQL_PASSWORD", ""),
      mysqlTable = mysqlTable,
      mysqlEnabled = mysqlEnabled)
  }
}

/**
 * 实时数据处理工作流
 * Kafka → Spark Streaming → MySQL
 */
object RealtimeWorkflow {

  private[bigdata] val RatingSchema = new StructType()
    .add("userId", DataTypes.IntegerType)
    .add("movieId", DataTypes.IntegerType)
    .add("rating", DataTypes.DoubleType)
    .add("timestamp", DataTypes.LongType)

  def main(args: Array[String]): Unit = {
    val config = RealtimeConfig.fromEnvironment()
    val spark = SparkSession.builder()
      .appName("RealtimeWorkflow")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    // 定义 Kafka 数据源
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBootstrapServers)
      .option("subscribe", config.kafkaTopic)
      .option("startingOffsets", "latest")
      .load()

    val ratings = parseRatings(
      kafkaStream.selectExpr("CAST(value AS STRING) as json"))
    val windowedStats = aggregateRatings(ratings)

    // 输出到 Parquet 和可选的 MySQL
    val query: StreamingQuery = windowedStats.writeStream
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        batchDF.write.mode(SaveMode.Append).parquet(config.outputPath)

        if (config.mysqlEnabled) {
          val props =
            MySQLExporter.createProperties(
              config.mysqlUser,
              config.mysqlPassword)
          MySQLExporter.exportToMySQL(
            batchDF,
            config.mysqlTable,
            config.jdbcUrl,
            props,
            SaveMode.Append)
        }
      }
      .option("checkpointLocation", config.checkpointPath)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    println("实时数据处理工作流已启动")
    println(s"Kafka Topic: ${config.kafkaTopic}")
    println(s"输出目录: ${config.outputPath}")
    println(
      if (config.mysqlEnabled) s"MySQL: ${config.jdbcUrl}/${config.mysqlTable}"
      else "MySQL: disabled")

    query.awaitTermination()
  }

  private[bigdata] def parseRatings(jsonValues: DataFrame): DataFrame = {
    jsonValues
      .select(from_json(col("json"), RatingSchema).as("data"))
      .select(
        col("data.userId").as("userId"),
        col("data.movieId").as("movieId"),
        col("data.rating").as("rating"),
        col("data.timestamp").cast(DataTypes.LongType).as("event_time")
      )
      .filter(
        col("userId").isNotNull &&
          col("movieId").isNotNull &&
          col("rating").isNotNull &&
          col("event_time").isNotNull)
      .withColumn(
        "event_timestamp",
        to_timestamp(from_unixtime(col("event_time"))))
  }

  private[bigdata] def aggregateRatings(ratings: DataFrame): DataFrame = {
    ratings
      .withWatermark("event_timestamp", "1 minute")
      .groupBy(
        window(col("event_timestamp"), "1 minute"),
        col("movieId")
      )
      .agg(
        count("*").as("rating_count"),
        round(avg("rating"), 2).as("avg_rating"),
        max("rating").as("max_rating"),
        min("rating").as("min_rating")
      )
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("movieId"),
        col("rating_count"),
        col("avg_rating"),
        col("max_rating"),
        col("min_rating")
      )
  }
}
