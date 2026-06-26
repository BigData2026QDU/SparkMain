package org.bigdata.streaming

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.bigdata.utils.MySQLExporter

/**
 * 实时数据处理工作流
 * Kafka → Spark Streaming → MySQL
 */
object RealtimeWorkflow {

  private val KAFKA_TOPIC = "ratings"
  private val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
  private val CHECKPOINT_PATH = "/tmp/spark/checkpoints/realtime_workflow"
  private val OUTPUT_PATH = "output/realtime_stats"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RealtimeWorkflow")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    // MySQL 配置
    val jdbcUrl = sys.env.getOrElse("MYSQL_JDBC_URL", "jdbc:mysql://localhost:3306/bigdata_ana")
    val mysqlUser = sys.env.getOrElse("MYSQL_USER", "root")
    val mysqlPassword = sys.env.getOrElse("MYSQL_PASSWORD", "")

    // 定义 Kafka 数据源
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "latest")
      .load()

    // 定义 JSON 数据结构
    val ratingSchema = new StructType()
      .add("userId", DataTypes.IntegerType)
      .add("movieId", DataTypes.IntegerType)
      .add("rating", DataTypes.DoubleType)
      .add("timestamp", DataTypes.LongType)

    // 解析 JSON 数据
    val ratings = kafkaStream
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", ratingSchema).as("data"))
      .select(
        $"data.userId",
        $"data.movieId",
        $"data.rating",
        $"data.timestamp".cast(DataTypes.LongType).as("event_time")
      )
      .withColumn("event_timestamp",($"event_time" * 1000).cast("timestamp"))

    // 窗口聚合：每分钟统计评分数量和平均评分
    val windowedStats = ratings
      .withWatermark("event_timestamp", "1 minute")
      .groupBy(
        window($"event_timestamp", "1 minute"),
        $"movieId"
      )
      .agg(
        count("*").as("rating_count"),
        round(avg("rating"), 2).as("avg_rating"),
        max("rating").as("max_rating"),
        min("rating").as("min_rating")
      )
      .select(
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"movieId",
        $"rating_count",
        $"avg_rating",
        $"max_rating",
        $"min_rating"
      )

    // 输出到 Parquet 和 MySQL
    val query: StreamingQuery = windowedStats.writeStream
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        // 写入 Parquet
        batchDF.write.mode(SaveMode.Append).parquet(OUTPUT_PATH)

        // 写入 MySQL
        if (jdbcUrl.nonEmpty && mysqlUser.nonEmpty) {
          val props = MySQLExporter.createProperties(mysqlUser, mysqlPassword)
          MySQLExporter.exportToMySQL(batchDF, "realtime_stats", jdbcUrl, props, SaveMode.Append)
        }
      }
      .option("checkpointLocation", CHECKPOINT_PATH)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    println("实时数据处理工作流已启动")
    println(s"Kafka Topic: $KAFKA_TOPIC")
    println(s"输出目录: $OUTPUT_PATH")
    println(s"MySQL: $jdbcUrl")

    query.awaitTermination()
  }
}
