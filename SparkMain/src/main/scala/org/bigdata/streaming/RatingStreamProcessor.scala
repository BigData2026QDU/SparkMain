package org.bigdata.streaming

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._

/**
 * 评分数据流处理器
 * 从 Kafka 消费评分数据，增量处理后写入 Parquet 文件
 */
object RatingStreamProcessor {

  private val KAFKA_TOPIC = "ratings"
  private val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
  private val CHECKPOINT_PATH = "/tmp/spark/checkpoints/ratings"
  private val OUTPUT_PATH = "output/ratings_streaming"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RatingStreamProcessor")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

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
      .select("data.*")

    // 添加处理时间戳（用于增量处理）
    val ratingsWithTimestamp = ratings
      .withColumn("process_time", current_timestamp())

    // 写入 Parquet 文件（增量合并）
    val query: StreamingQuery = ratingsWithTimestamp.writeStream
      .outputMode("append")
      .format("parquet")
      .option("checkpointLocation", CHECKPOINT_PATH)
      .option("path", OUTPUT_PATH)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}
