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

      val categories = UserBehaviorRealtimeJob
        .aggregateObjects(windowed, "category_id")
        .collect()
      assert(categories.length == 1)
      assert(categories.head.getAs[Long]("event_cnt") == 6L)
      assert(categories.head.getAs[Long]("buy_cnt") == 2L)

      val items = UserBehaviorRealtimeJob
        .aggregateObjects(windowed, "item_id")
        .orderBy(col("item_id"))
        .collect()
      assert(items.length == 2)
      assert(items.forall(_.getAs[Long]("event_cnt") == 3L))

      println("[SUCCESS] UserBehaviorRealtimeSmokeTest passed")
    } finally {
      spark.stop()
    }
  }
}
