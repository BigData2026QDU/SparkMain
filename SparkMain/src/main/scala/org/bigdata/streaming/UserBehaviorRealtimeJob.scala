package org.bigdata.streaming

import java.sql.{Connection, DriverManager, Timestamp}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Issue #19: Kafka -> Spark Structured Streaming -> MySQL -> Blog.
 *
 * Historical behavior logs are replayed through Kafka and aggregated into
 * five-minute event-time windows. The public result contains only PV, favorite,
 * cart, purchase counts, and anomaly alerts.
 */
object UserBehaviorRealtimeJob {

  case class Config(
      topic: String,
      bootstrapServers: String,
      checkpointPath: String,
      startingOffsets: String,
      triggerInterval: String,
      jdbcUrl: String,
      mysqlUser: String,
      mysqlPassword: String,
      minimumTraffic: Long)

  def main(args: Array[String]): Unit = {
    val config = loadConfig(args)
    val spark = SparkSession.builder()
      .appName("LuckyAnJunUserBehaviorRealtime")
      .config("spark.sql.shuffle.partitions", env("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
      .getOrCreate()

    spark.sparkContext.setLogLevel(env("SPARK_LOG_LEVEL", "WARN"))
    MySQLBatchSink.ensureSchema(config)

    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.bootstrapServers)
      .option("subscribe", config.topic)
      .option("startingOffsets", config.startingOffsets)
      .option("failOnDataLoss", "false")
      .load()

    val query = parseKafka(kafka).writeStream
      .queryName("luckyanjun_user_behavior_realtime")
      .outputMode("append")
      .option("checkpointLocation", config.checkpointPath)
      .trigger(Trigger.ProcessingTime(config.triggerInterval))
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        MySQLBatchSink.write(batch, batchId, config)
      }
      .start()

    println("[STARTED] LuckyAnJun realtime analysis")
    println("[INFO] Kafka: " + config.bootstrapServers + "/" + config.topic)
    println("[INFO] Checkpoint: " + config.checkpointPath)
    println("[INFO] MySQL: " + config.jdbcUrl)
    println("[INFO] Blog table: lb_realtime_blog_metrics")
    query.awaitTermination()
  }

  def parseKafka(kafka: DataFrame): DataFrame = {
    val schema = new StructType()
      .add("behavior_type", StringType)
      .add("timestamp", LongType)

    kafka
      .select(from_json(col("value").cast(StringType), schema).as("data"))
      .select(
        lower(col("data.behavior_type")).as("behavior_type"),
        col("data.timestamp").as("timestamp"))
      .filter(
        col("timestamp").gt(0L) &&
          col("behavior_type").isin("pv", "fav", "cart", "buy"))
      .withColumn("event_time", from_unixtime(col("timestamp")).cast(TimestampType))
  }

  def withFiveMinuteWindow(events: DataFrame): DataFrame = {
    events
      .withColumn("event_window", window(col("event_time"), "5 minutes"))
      .withColumn("window_start", col("event_window.start"))
      .withColumn("window_end", col("event_window.end"))
      .drop("event_window")
  }

  def aggregateMetrics(windowed: DataFrame): DataFrame = {
    windowed.groupBy("window_start", "window_end")
      .agg(
        sum(when(col("behavior_type") === "pv", 1L).otherwise(0L)).as("pv"),
        sum(when(col("behavior_type") === "fav", 1L).otherwise(0L)).as("fav_cnt"),
        sum(when(col("behavior_type") === "cart", 1L).otherwise(0L)).as("cart_cnt"),
        sum(when(col("behavior_type") === "buy", 1L).otherwise(0L)).as("buy_cnt"))
  }

  def classifyAlert(
      pv: Long,
      buyCount: Long,
      previousPv: Option[Long],
      minimumTraffic: Long): (String, String) = {
    if (previousPv.exists(p => p >= minimumTraffic && pv >= p * 2L)) {
      ("traffic_spike", "PV 至少是上一窗口的 2 倍")
    } else if (previousPv.exists(p => p >= minimumTraffic && pv * 2L < p)) {
      ("traffic_drop", "PV 低于上一窗口的一半")
    } else if (pv < minimumTraffic) {
      ("low_traffic", "PV 低于配置的最低流量")
    } else if (buyCount * 100L < pv) {
      ("low_conversion", "购买次数与 PV 的比例低于 1%")
    } else {
      ("normal", "")
    }
  }

  private def loadConfig(args: Array[String]): Config = {
    Config(
      topic = arg(args, 0, env("KAFKA_USER_BEHAVIOR_TOPIC", "taobao_behavior")),
      bootstrapServers = arg(args, 1, env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")),
      checkpointPath = arg(args, 2, env(
        "USER_BEHAVIOR_CHECKPOINT",
        "/tmp/spark/checkpoints/luckyanjun_user_behavior")),
      startingOffsets = env("KAFKA_STARTING_OFFSETS", "latest"),
      triggerInterval = env("STREAMING_TRIGGER_INTERVAL", "10 seconds"),
      jdbcUrl = env(
        "MYSQL_JDBC_URL",
        "jdbc:mysql://localhost:3306/bigdata_ana?useUnicode=true&characterEncoding=utf8&useSSL=false"),
      mysqlUser = env("MYSQL_USER", "root"),
      mysqlPassword = env("MYSQL_PASSWORD", ""),
      minimumTraffic = env("REALTIME_MINIMUM_TRAFFIC", "5").toLong)
  }

  private def arg(args: Array[String], index: Int, defaultValue: String): String = {
    if (args.length > index && args(index).nonEmpty) args(index) else defaultValue
  }

  private def env(name: String, defaultValue: String): String = {
    sys.env.get(name).filter(_.nonEmpty).getOrElse(defaultValue)
  }
}

private object MySQLBatchSink {

  private val DdlStatements = Seq(
    """CREATE TABLE IF NOT EXISTS lb_realtime_batches (
      |  checkpoint_key VARCHAR(255) NOT NULL,
      |  batch_id BIGINT NOT NULL,
      |  processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      |  PRIMARY KEY (checkpoint_key, batch_id)
      |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""".stripMargin,
    """CREATE TABLE IF NOT EXISTS lb_realtime_window_metrics (
      |  window_start DATETIME NOT NULL,
      |  window_end DATETIME NOT NULL,
      |  pv BIGINT NOT NULL DEFAULT 0,
      |  fav_cnt BIGINT NOT NULL DEFAULT 0,
      |  cart_cnt BIGINT NOT NULL DEFAULT 0,
      |  buy_cnt BIGINT NOT NULL DEFAULT 0,
      |  alert_type VARCHAR(32) NOT NULL DEFAULT 'normal',
      |  alert_message VARCHAR(255) NOT NULL DEFAULT '',
      |  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      |  PRIMARY KEY (window_start)
      |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""".stripMargin,
    """CREATE TABLE IF NOT EXISTS lb_realtime_blog_metrics (
      |  window_start DATETIME NOT NULL,
      |  window_label VARCHAR(32) NOT NULL,
      |  pv BIGINT NOT NULL DEFAULT 0,
      |  fav_cnt BIGINT NOT NULL DEFAULT 0,
      |  cart_cnt BIGINT NOT NULL DEFAULT 0,
      |  buy_cnt BIGINT NOT NULL DEFAULT 0,
      |  alert_level INT NOT NULL DEFAULT 0,
      |  alert_type VARCHAR(32) NOT NULL DEFAULT 'normal',
      |  alert_message VARCHAR(255) NOT NULL DEFAULT '',
      |  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      |  PRIMARY KEY (window_start)
      |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""".stripMargin)

  def ensureSchema(config: UserBehaviorRealtimeJob.Config): Unit = {
    withConnection(config) { connection =>
      val statement = connection.createStatement()
      try DdlStatements.foreach(statement.execute)
      finally statement.close()
    }
  }

  def write(batch: DataFrame, batchId: Long, config: UserBehaviorRealtimeJob.Config): Unit = {
    if (batch.limit(1).count() == 0L) {
      return
    }

    val windowed = UserBehaviorRealtimeJob.withFiveMinuteWindow(batch).cache()
    val metrics = UserBehaviorRealtimeJob.aggregateMetrics(windowed).collect()
    val affectedWindows = metrics.map(_.getAs[Timestamp]("window_start")).distinct.sortBy(_.getTime)

    try {
      val connection = openConnection(config)
      connection.setAutoCommit(false)
      try {
        if (!claimBatch(connection, config.checkpointPath, batchId)) {
          connection.rollback()
          connection.close()
          return
        }

        upsertMetrics(connection, metrics)
        affectedWindows.foreach(refreshAlert(connection, _, config.minimumTraffic))
        refreshBlogMetrics(connection)

        connection.commit()
        connection.close()
        println("[BATCH] id=" + batchId +
          ", events=" + batch.count() +
          ", windows=" + affectedWindows.length)
      } catch {
        case error: Throwable =>
          try connection.rollback() catch { case _: Throwable => }
          try connection.close() catch { case _: Throwable => }
          throw error
      }
    } finally {
      windowed.unpersist()
    }
  }

  private def claimBatch(
      connection: Connection,
      checkpointKey: String,
      batchId: Long): Boolean = {
    val statement = connection.prepareStatement(
      "INSERT IGNORE INTO lb_realtime_batches (checkpoint_key, batch_id) VALUES (?, ?)")
    try {
      statement.setString(1, checkpointKey)
      statement.setLong(2, batchId)
      statement.executeUpdate() == 1
    } finally {
      statement.close()
    }
  }

  private def upsertMetrics(connection: Connection, rows: Array[Row]): Unit = {
    val statement = connection.prepareStatement(
      """INSERT INTO lb_realtime_window_metrics
        |  (window_start, window_end, pv, fav_cnt, cart_cnt, buy_cnt)
        |VALUES (?, ?, ?, ?, ?, ?)
        |ON DUPLICATE KEY UPDATE
        |  window_end = VALUES(window_end),
        |  pv = pv + VALUES(pv),
        |  fav_cnt = fav_cnt + VALUES(fav_cnt),
        |  cart_cnt = cart_cnt + VALUES(cart_cnt),
        |  buy_cnt = buy_cnt + VALUES(buy_cnt)""".stripMargin)
    try {
      rows.foreach { row =>
        statement.setTimestamp(1, row.getAs[Timestamp]("window_start"))
        statement.setTimestamp(2, row.getAs[Timestamp]("window_end"))
        statement.setLong(3, row.getAs[Long]("pv"))
        statement.setLong(4, row.getAs[Long]("fav_cnt"))
        statement.setLong(5, row.getAs[Long]("cart_cnt"))
        statement.setLong(6, row.getAs[Long]("buy_cnt"))
        statement.addBatch()
      }
      statement.executeBatch()
    } finally {
      statement.close()
    }
  }

  private def refreshAlert(
      connection: Connection,
      windowStart: Timestamp,
      minimumTraffic: Long): Unit = {
    val current = connection.prepareStatement(
      """SELECT pv, buy_cnt
        |FROM lb_realtime_window_metrics WHERE window_start = ?""".stripMargin)
    val previous = connection.prepareStatement(
      """SELECT pv FROM lb_realtime_window_metrics
        |WHERE window_start < ? ORDER BY window_start DESC LIMIT 1""".stripMargin)
    try {
      current.setTimestamp(1, windowStart)
      val currentResult = current.executeQuery()
      currentResult.next()
      val pv = currentResult.getLong("pv")
      val buyCount = currentResult.getLong("buy_cnt")
      currentResult.close()

      previous.setTimestamp(1, windowStart)
      val previousResult = previous.executeQuery()
      val previousPv = if (previousResult.next()) Some(previousResult.getLong("pv")) else None
      previousResult.close()

      val alert = UserBehaviorRealtimeJob.classifyAlert(
        pv,
        buyCount,
        previousPv,
        minimumTraffic)

      val update = connection.prepareStatement(
        """UPDATE lb_realtime_window_metrics
          |SET alert_type = ?, alert_message = ? WHERE window_start = ?""".stripMargin)
      try {
        update.setString(1, alert._1)
        update.setString(2, alert._2)
        update.setTimestamp(3, windowStart)
        update.executeUpdate()
      } finally {
        update.close()
      }
    } finally {
      current.close()
      previous.close()
    }
  }

  private def refreshBlogMetrics(connection: Connection): Unit = {
    val delete = connection.prepareStatement("DELETE FROM lb_realtime_blog_metrics")
    try {
      delete.executeUpdate()
    } finally {
      delete.close()
    }

    val insert = connection.prepareStatement(
      """INSERT INTO lb_realtime_blog_metrics
        |  (window_start, window_label, pv, fav_cnt, cart_cnt, buy_cnt,
        |   alert_level, alert_type, alert_message, updated_at)
        |SELECT
        |  recent.window_start,
        |  DATE_FORMAT(recent.window_start, '%m-%d %H:%i'),
        |  recent.pv,
        |  recent.fav_cnt,
        |  recent.cart_cnt,
        |  recent.buy_cnt,
        |  CASE
        |    WHEN recent.alert_type = 'normal' THEN 0
        |    WHEN recent.alert_type IN ('low_traffic', 'low_conversion') THEN 1
        |    ELSE 2
        |  END,
        |  recent.alert_type,
        |  recent.alert_message,
        |  recent.updated_at
        |FROM (
        |  SELECT window_start, pv, fav_cnt, cart_cnt, buy_cnt,
        |         alert_type, alert_message, updated_at
        |  FROM lb_realtime_window_metrics
        |  ORDER BY updated_at DESC, window_start DESC
        |  LIMIT 12
        |) recent
        |ORDER BY recent.window_start""".stripMargin)
    try {
      insert.executeUpdate()
    } finally {
      insert.close()
    }
  }

  private def withConnection(
      config: UserBehaviorRealtimeJob.Config)(operation: Connection => Unit): Unit = {
    val connection = openConnection(config)
    try operation(connection) finally connection.close()
  }

  private def openConnection(config: UserBehaviorRealtimeJob.Config): Connection = {
    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
    } catch {
      case _: ClassNotFoundException => Class.forName("com.mysql.jdbc.Driver")
    }
    DriverManager.getConnection(config.jdbcUrl, config.mysqlUser, config.mysqlPassword)
  }
}
