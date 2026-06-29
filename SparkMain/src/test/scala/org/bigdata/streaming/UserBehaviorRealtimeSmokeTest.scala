package org.bigdata.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object UserBehaviorRealtimeSmokeTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("UserBehaviorRealtimeSmokeTest")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._

    try {
      val events = Seq(
        (1L, 1001L, 501L, "pv", 1511544010L),
        (1L, 1001L, 501L, "cart", 1511544020L),
        (1L, 1001L, 501L, "buy", 1511544030L),
        (2L, 1002L, 501L, "pv", 1511544040L),
        (2L, 1002L, 501L, "fav", 1511544050L),
        (2L, 1002L, 501L, "buy", 1511544060L))
        .toDF("user_id", "item_id", "category_id", "behavior_type", "timestamp")
        .withColumn("event_time", from_unixtime(col("timestamp")).cast(TimestampType))

      val windowed = UserBehaviorRealtimeJob.withFiveMinuteWindow(events)
      val metrics = UserBehaviorRealtimeJob.aggregateMetrics(windowed).collect()
      assert(metrics.length == 1, "Expected one five-minute window")

      val row = metrics.head
      assert(row.getAs[Long]("pv") == 2L)
      assert(row.getAs[Long]("fav_cnt") == 1L)
      assert(row.getAs[Long]("cart_cnt") == 1L)
      assert(row.getAs[Long]("buy_cnt") == 2L)

      assert(UserBehaviorRealtimeJob.classifyAlert(20L, 1L, Some(5L), 5L)._1 == "traffic_spike")
      assert(UserBehaviorRealtimeJob.classifyAlert(4L, 1L, Some(20L), 5L)._1 == "traffic_drop")
      assert(UserBehaviorRealtimeJob.classifyAlert(4L, 0L, None, 5L)._1 == "low_traffic")
      assert(UserBehaviorRealtimeJob.classifyAlert(200L, 1L, Some(150L), 5L)._1 == "low_conversion")
      assert(UserBehaviorRealtimeJob.classifyAlert(100L, 1L, Some(100L), 5L)._1 == "normal")

      println("[SUCCESS] UserBehaviorRealtimeSmokeTest passed")
    } finally {
      spark.stop()
    }
  }
}
