package org.example.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

/**
 * Computes 5-minute business metrics from Taobao UserBehavior Kafka events.
 */
public class UserBehaviorStreamProcessor {

    private static final String KAFKA_TOPIC = "taobao_behavior";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CHECKPOINT_PATH = "/tmp/spark/checkpoints/taobao_behavior";

    public static void main(String[] args) throws Exception {
        String topic = args.length > 0 ? args[0] : KAFKA_TOPIC;
        String bootstrapServers = args.length > 1 ? args[1] : KAFKA_BOOTSTRAP_SERVERS;

        SparkSession spark = SparkSession.builder()
                .appName("UserBehaviorStreamProcessor")
                .config("spark.sql.shuffle.partitions", "4")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", "latest")
                .load();

        StructType schema = new StructType()
                .add("user_id", DataTypes.LongType)
                .add("item_id", DataTypes.LongType)
                .add("category_id", DataTypes.LongType)
                .add("behavior_type", DataTypes.StringType)
                .add("timestamp", DataTypes.LongType);

        Dataset<Row> events = kafkaStream
                .selectExpr("CAST(value AS STRING) AS json")
                .select(from_json(col("json"), schema).as("data"))
                .select("data.*")
                .filter(col("behavior_type").isin("pv", "buy", "cart", "fav"))
                .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))
                .withWatermark("event_time", "10 minutes");

        Dataset<Row> windowMetrics = events
                .groupBy(window(col("event_time"), "5 minutes"))
                .agg(
                        count(when(col("behavior_type").equalTo("pv"), 1)).as("pv"),
                        approx_count_distinct(when(col("behavior_type").equalTo("pv"), col("user_id"))).as("approx_uv"),
                        count(when(col("behavior_type").equalTo("fav"), 1)).as("fav_cnt"),
                        count(when(col("behavior_type").equalTo("cart"), 1)).as("cart_cnt"),
                        count(when(col("behavior_type").equalTo("buy"), 1)).as("buy_cnt"),
                        approx_count_distinct(when(col("behavior_type").equalTo("buy"), col("user_id"))).as("buy_uv")
                )
                .withColumn("pv_to_buy_rate", round(col("buy_uv").divide(col("approx_uv")), 4))
                .withColumn("alert_type",
                        when(col("pv").lt(5), lit("low_traffic"))
                                .when(col("pv_to_buy_rate").lt(0.05), lit("low_conversion"))
                                .otherwise(lit("normal")));

        Dataset<Row> categoryTop = events
                .groupBy(window(col("event_time"), "5 minutes"), col("category_id"))
                .agg(count(lit(1)).as("event_cnt"))
                .orderBy(col("window"), col("event_cnt").desc());

        Dataset<Row> itemTop = events
                .groupBy(window(col("event_time"), "5 minutes"), col("item_id"))
                .agg(count(lit(1)).as("event_cnt"))
                .orderBy(col("window"), col("event_cnt").desc());

        windowMetrics.writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", "false")
                .option("checkpointLocation", CHECKPOINT_PATH + "/metrics")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        categoryTop.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .option("checkpointLocation", CHECKPOINT_PATH + "/category_top")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        itemTop.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .option("checkpointLocation", CHECKPOINT_PATH + "/item_top")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start()
                .awaitTermination();
    }
}
