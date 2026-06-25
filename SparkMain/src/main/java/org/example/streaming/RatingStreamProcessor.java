package org.example.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.*;

/**
 * 评分数据流处理器
 * 从 Kafka 消费评分数据，增量处理后写入 Hive
 */
public class RatingStreamProcessor {

    private static final String KAFKA_TOPIC = "ratings";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CHECKPOINT_PATH = "/tmp/spark/checkpoints/ratings";
    private static final String HIVE_TABLE = "bigdata_ana.ratings_streaming";

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("RatingStreamProcessor")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();

        // 定义 Kafka 数据源
        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "latest")
                .load();

        // 定义 JSON 数据结构
        Schema ratingSchema = new StructType()
                .add("userId", DataTypes.IntegerType)
                .add("movieId", DataTypes.IntegerType)
                .add("rating", DataTypes.DoubleType)
                .add("timestamp", DataTypes.LongType);

        // 解析 JSON 数据
        Dataset<Row> ratings = kafkaStream
                .selectExpr("CAST(value AS STRING) as json")
                .select(org.apache.spark.sql.functions
                        .from_json(org.apache.spark.sql.functions.col("json"), ratingSchema)
                        .as("data"))
                .select("data.*");

        // 添加处理时间戳（用于增量处理）
        Dataset<Row> ratingsWithTimestamp = ratings
                .withColumn("process_time", org.apache.spark.sql.functions.current_timestamp());

        // 写入 Hive 表（增量合并）
        StreamingQuery query = ratingsWithTimestamp.writeStream()
                .outputMode("append")
                .format("hive")
                .option("checkpointLocation", CHECKPOINT_PATH)
                .option("path", "/user/hive/warehouse/bigdata_ana.db/ratings_streaming")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        query.awaitTermination();
    }
}
