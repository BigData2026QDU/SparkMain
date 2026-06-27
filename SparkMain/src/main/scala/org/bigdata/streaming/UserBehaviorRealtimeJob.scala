package org.bigdata.streaming

import java.nio.charset.StandardCharsets
import java.nio.file.{AtomicMoveNotSupportedException, Files, Paths, StandardCopyOption}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Timestamp}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * Issue #19: Kafka -> Spark Structured Streaming -> MySQL and JSON snapshot.
 *
 * The input is a historical log replay. Event timestamps are preserved and
 * grouped into five-minute event-time windows.
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
      snapshotPath: String,
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

    val events = parseKafka(kafka)
    val query = events.writeStream
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
    println("[INFO] Snapshot: " + config.snapshotPath)
    query.awaitTermination()
  }

  def parseKafka(kafka: DataFrame): DataFrame = {
    val schema = new StructType()
      .add("user_id", LongType)
      .add("item_id", LongType)
      .add("category_id", LongType)
      .add("behavior_type", StringType)
      .add("timestamp", LongType)

    kafka
      .select(
        col("partition").cast(IntegerType).as("kafka_partition"),
        col("offset").cast(LongType).as("kafka_offset"),
        col("value").cast(StringType).as("json"))
      .select(
        col("kafka_partition"),
        col("kafka_offset"),
        from_json(col("json"), schema).as("data"))
      .select(
        col("kafka_partition"),
        col("kafka_offset"),
        col("data.user_id").as("user_id"),
        col("data.item_id").as("item_id"),
        col("data.category_id").as("category_id"),
        lower(col("data.behavior_type")).as("behavior_type"),
        col("data.timestamp").as("timestamp"))
      .filter(
        col("user_id").isNotNull &&
          col("item_id").isNotNull &&
          col("category_id").isNotNull &&
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

  def aggregateObjects(windowed: DataFrame, idColumn: String): DataFrame = {
    windowed.groupBy("window_start", "window_end", idColumn)
      .agg(
        count(lit(1)).as("event_cnt"),
        sum(when(col("behavior_type") === "buy", 1L).otherwise(0L)).as("buy_cnt"))
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
      snapshotPath = env("REALTIME_SNAPSHOT_PATH", "web/data/realtime.json"),
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
    """CREATE TABLE IF NOT EXISTS lb_realtime_window_users (
      |  window_start DATETIME NOT NULL,
      |  user_id BIGINT NOT NULL,
      |  is_pv_user TINYINT NOT NULL DEFAULT 0,
      |  is_buy_user TINYINT NOT NULL DEFAULT 0,
      |  PRIMARY KEY (window_start, user_id)
      |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""".stripMargin,
    """CREATE TABLE IF NOT EXISTS lb_realtime_window_metrics (
      |  window_start DATETIME NOT NULL,
      |  window_end DATETIME NOT NULL,
      |  pv BIGINT NOT NULL DEFAULT 0,
      |  approx_uv BIGINT NOT NULL DEFAULT 0,
      |  fav_cnt BIGINT NOT NULL DEFAULT 0,
      |  cart_cnt BIGINT NOT NULL DEFAULT 0,
      |  buy_cnt BIGINT NOT NULL DEFAULT 0,
      |  buy_uv BIGINT NOT NULL DEFAULT 0,
      |  pv_to_buy_rate DECIMAL(12,4) NOT NULL DEFAULT 0,
      |  alert_type VARCHAR(32) NOT NULL DEFAULT 'normal',
      |  alert_message VARCHAR(255) NOT NULL DEFAULT '',
      |  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      |  PRIMARY KEY (window_start)
      |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""".stripMargin,
    objectStatsDdl("lb_realtime_category_stats", "category_id"),
    objectStatsDdl("lb_realtime_item_stats", "item_id"),
    topDdl("lb_realtime_category_top10", "category_id"),
    topDdl("lb_realtime_item_top10", "item_id"))

  def ensureSchema(config: UserBehaviorRealtimeJob.Config): Unit = {
    withConnection(config) { connection =>
      val statement = connection.createStatement()
      try {
        DdlStatements.foreach(statement.execute)
      } finally {
        statement.close()
      }
    }
  }

  def write(batch: DataFrame, batchId: Long, config: UserBehaviorRealtimeJob.Config): Unit = {
    if (batch.limit(1).count() == 0L) {
      return
    }

    val windowed = UserBehaviorRealtimeJob.withFiveMinuteWindow(batch).cache()
    val metrics = UserBehaviorRealtimeJob.aggregateMetrics(windowed).collect()
    val categories = UserBehaviorRealtimeJob.aggregateObjects(windowed, "category_id").collect()
    val items = UserBehaviorRealtimeJob.aggregateObjects(windowed, "item_id").collect()
    val users = windowed
      .groupBy("window_start", "user_id")
      .agg(
        max(when(col("behavior_type") === "pv", 1).otherwise(0)).as("is_pv_user"),
        max(when(col("behavior_type") === "buy", 1).otherwise(0)).as("is_buy_user"))
      .collect()

    val affectedWindows = metrics.map(_.getAs[Timestamp]("window_start")).distinct.sortBy(_.getTime)

    try {
      val connection = openConnection(config)
      connection.setAutoCommit(false)
      try {
        if (!claimBatch(connection, config.checkpointPath, batchId)) {
          connection.rollback()
          connection.close()
          writeSnapshot(config)
          return
        }

        upsertMetrics(connection, metrics)
        upsertUsers(connection, users)
        upsertObjectStats(connection, categories, "lb_realtime_category_stats", "category_id")
        upsertObjectStats(connection, items, "lb_realtime_item_stats", "item_id")

        affectedWindows.foreach { windowStart =>
          refreshDistinctUsers(connection, windowStart)
          refreshTop10(
            connection,
            windowStart,
            "lb_realtime_category_stats",
            "lb_realtime_category_top10",
            "category_id")
          refreshTop10(
            connection,
            windowStart,
            "lb_realtime_item_stats",
            "lb_realtime_item_top10",
            "item_id")
          refreshAlert(connection, windowStart, config.minimumTraffic)
        }

        connection.commit()
        connection.close()
        writeSnapshot(config)
        println("[BATCH] id=" + batchId +
          ", events=" + batch.count() +
          ", windows=" + affectedWindows.length +
          ", categories=" + categories.length +
          ", items=" + items.length)
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
    val sql =
      "INSERT IGNORE INTO lb_realtime_batches (checkpoint_key, batch_id) VALUES (?, ?)"
    val statement = connection.prepareStatement(sql)
    try {
      statement.setString(1, checkpointKey)
      statement.setLong(2, batchId)
      statement.executeUpdate() == 1
    } finally {
      statement.close()
    }
  }

  private def upsertMetrics(connection: Connection, rows: Array[Row]): Unit = {
    val sql =
      """INSERT INTO lb_realtime_window_metrics
        |  (window_start, window_end, pv, fav_cnt, cart_cnt, buy_cnt)
        |VALUES (?, ?, ?, ?, ?, ?)
        |ON DUPLICATE KEY UPDATE
        |  window_end = VALUES(window_end),
        |  pv = pv + VALUES(pv),
        |  fav_cnt = fav_cnt + VALUES(fav_cnt),
        |  cart_cnt = cart_cnt + VALUES(cart_cnt),
        |  buy_cnt = buy_cnt + VALUES(buy_cnt)""".stripMargin
    val statement = connection.prepareStatement(sql)
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

  private def upsertUsers(connection: Connection, rows: Array[Row]): Unit = {
    val sql =
      """INSERT INTO lb_realtime_window_users
        |  (window_start, user_id, is_pv_user, is_buy_user)
        |VALUES (?, ?, ?, ?)
        |ON DUPLICATE KEY UPDATE
        |  is_pv_user = GREATEST(is_pv_user, VALUES(is_pv_user)),
        |  is_buy_user = GREATEST(is_buy_user, VALUES(is_buy_user))""".stripMargin
    val statement = connection.prepareStatement(sql)
    try {
      rows.foreach { row =>
        statement.setTimestamp(1, row.getAs[Timestamp]("window_start"))
        statement.setLong(2, row.getAs[Long]("user_id"))
        statement.setInt(3, row.getAs[Int]("is_pv_user"))
        statement.setInt(4, row.getAs[Int]("is_buy_user"))
        statement.addBatch()
      }
      statement.executeBatch()
    } finally {
      statement.close()
    }
  }

  private def upsertObjectStats(
      connection: Connection,
      rows: Array[Row],
      table: String,
      idColumn: String): Unit = {
    val sql =
      s"""INSERT INTO $table
         |  (window_start, window_end, $idColumn, event_cnt, buy_cnt)
         |VALUES (?, ?, ?, ?, ?)
         |ON DUPLICATE KEY UPDATE
         |  window_end = VALUES(window_end),
         |  event_cnt = event_cnt + VALUES(event_cnt),
         |  buy_cnt = buy_cnt + VALUES(buy_cnt)""".stripMargin
    val statement = connection.prepareStatement(sql)
    try {
      rows.foreach { row =>
        statement.setTimestamp(1, row.getAs[Timestamp]("window_start"))
        statement.setTimestamp(2, row.getAs[Timestamp]("window_end"))
        statement.setLong(3, row.getAs[Long](idColumn))
        statement.setLong(4, row.getAs[Long]("event_cnt"))
        statement.setLong(5, row.getAs[Long]("buy_cnt"))
        statement.addBatch()
      }
      statement.executeBatch()
    } finally {
      statement.close()
    }
  }

  private def refreshDistinctUsers(connection: Connection, windowStart: Timestamp): Unit = {
    val sql =
      """UPDATE lb_realtime_window_metrics
        |SET
        |  approx_uv = (
        |    SELECT COUNT(*) FROM lb_realtime_window_users u
        |    WHERE u.window_start = ? AND u.is_pv_user = 1
        |  ),
        |  buy_uv = (
        |    SELECT COUNT(*) FROM lb_realtime_window_users u
        |    WHERE u.window_start = ? AND u.is_buy_user = 1
        |  )
        |WHERE window_start = ?""".stripMargin
    val statement = connection.prepareStatement(sql)
    try {
      statement.setTimestamp(1, windowStart)
      statement.setTimestamp(2, windowStart)
      statement.setTimestamp(3, windowStart)
      statement.executeUpdate()
    } finally {
      statement.close()
    }

    val rateStatement = connection.prepareStatement(
      """UPDATE lb_realtime_window_metrics
        |SET pv_to_buy_rate =
        |  CASE WHEN approx_uv = 0 THEN 0 ELSE ROUND(buy_uv / approx_uv, 4) END
        |WHERE window_start = ?""".stripMargin)
    try {
      rateStatement.setTimestamp(1, windowStart)
      rateStatement.executeUpdate()
    } finally {
      rateStatement.close()
    }
  }

  private def refreshTop10(
      connection: Connection,
      windowStart: Timestamp,
      sourceTable: String,
      targetTable: String,
      idColumn: String): Unit = {
    val delete = connection.prepareStatement(
      s"DELETE FROM $targetTable WHERE window_start = ?")
    try {
      delete.setTimestamp(1, windowStart)
      delete.executeUpdate()
    } finally {
      delete.close()
    }

    val select = connection.prepareStatement(
      s"""SELECT window_end, $idColumn, event_cnt, buy_cnt
         |FROM $sourceTable
         |WHERE window_start = ?
         |ORDER BY event_cnt DESC, buy_cnt DESC, $idColumn ASC
         |LIMIT 10""".stripMargin)
    val insert = connection.prepareStatement(
      s"""INSERT INTO $targetTable
         |  (window_start, window_end, rank_no, $idColumn, event_cnt, buy_cnt)
         |VALUES (?, ?, ?, ?, ?, ?)""".stripMargin)
    try {
      select.setTimestamp(1, windowStart)
      val result = select.executeQuery()
      var rank = 1
      while (result.next()) {
        insert.setTimestamp(1, windowStart)
        insert.setTimestamp(2, result.getTimestamp("window_end"))
        insert.setInt(3, rank)
        insert.setLong(4, result.getLong(idColumn))
        insert.setLong(5, result.getLong("event_cnt"))
        insert.setLong(6, result.getLong("buy_cnt"))
        insert.addBatch()
        rank += 1
      }
      result.close()
      insert.executeBatch()
    } finally {
      select.close()
      insert.close()
    }
  }

  private def refreshAlert(
      connection: Connection,
      windowStart: Timestamp,
      minimumTraffic: Long): Unit = {
    val current = connection.prepareStatement(
      """SELECT pv, approx_uv, pv_to_buy_rate
        |FROM lb_realtime_window_metrics WHERE window_start = ?""".stripMargin)
    val previous = connection.prepareStatement(
      """SELECT pv FROM lb_realtime_window_metrics
        |WHERE window_start < ? ORDER BY window_start DESC LIMIT 1""".stripMargin)
    try {
      current.setTimestamp(1, windowStart)
      val currentResult = current.executeQuery()
      currentResult.next()
      val pv = currentResult.getLong("pv")
      val approxUv = currentResult.getLong("approx_uv")
      val conversionRate = currentResult.getDouble("pv_to_buy_rate")
      currentResult.close()

      previous.setTimestamp(1, windowStart)
      val previousResult = previous.executeQuery()
      val previousPv = if (previousResult.next()) Some(previousResult.getLong("pv")) else None
      previousResult.close()

      val alert =
        if (previousPv.exists(p => p >= minimumTraffic && pv >= p * 2L)) {
          ("traffic_spike", "PV is at least twice the previous window")
        } else if (previousPv.exists(p => p >= minimumTraffic && pv * 2L < p)) {
          ("traffic_drop", "PV is less than half of the previous window")
        } else if (approxUv >= 10L && conversionRate < 0.01d) {
          ("low_conversion", "PV-to-buy user conversion is below 1%")
        } else if (pv < minimumTraffic) {
          ("low_traffic", "PV is below the configured minimum")
        } else {
          ("normal", "")
        }

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

  private def writeSnapshot(config: UserBehaviorRealtimeJob.Config): Unit = {
    withConnection(config) { connection =>
      val latest = latestMetrics(connection)
      val json = latest match {
        case Some(metrics) =>
          val windowStart = metrics._1
          val categories = topRows(
            connection,
            "lb_realtime_category_top10",
            "category_id",
            windowStart)
          val items = topRows(
            connection,
            "lb_realtime_item_top10",
            "item_id",
            windowStart)
          renderSnapshot(metrics, categories, items)
        case None =>
          """{"status":"waiting","metrics":null,"category_top10":[],"item_top10":[]}"""
      }

      val target = Paths.get(config.snapshotPath).toAbsolutePath.normalize()
      val parent = target.getParent
      if (parent != null) {
        Files.createDirectories(parent)
      }
      val temp = target.resolveSibling(target.getFileName.toString + ".tmp")
      Files.write(temp, json.getBytes(StandardCharsets.UTF_8))
      try {
        Files.move(
          temp,
          target,
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE)
      } catch {
        case _: AtomicMoveNotSupportedException =>
          Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING)
      }
    }
  }

  private def latestMetrics(
      connection: Connection): Option[(Timestamp, Timestamp, Long, Long, Long, Long, Long, Long, Double, String, String)] = {
    val statement = connection.prepareStatement(
      """SELECT window_start, window_end, pv, approx_uv, fav_cnt, cart_cnt,
        |       buy_cnt, buy_uv, pv_to_buy_rate, alert_type, alert_message
        |FROM lb_realtime_window_metrics
        |ORDER BY window_start DESC LIMIT 1""".stripMargin)
    try {
      val result = statement.executeQuery()
      if (result.next()) {
        Some((
          result.getTimestamp("window_start"),
          result.getTimestamp("window_end"),
          result.getLong("pv"),
          result.getLong("approx_uv"),
          result.getLong("fav_cnt"),
          result.getLong("cart_cnt"),
          result.getLong("buy_cnt"),
          result.getLong("buy_uv"),
          result.getDouble("pv_to_buy_rate"),
          result.getString("alert_type"),
          result.getString("alert_message")))
      } else {
        None
      }
    } finally {
      statement.close()
    }
  }

  private def topRows(
      connection: Connection,
      table: String,
      idColumn: String,
      windowStart: Timestamp): Seq[(Int, Long, Long, Long)] = {
    val statement = connection.prepareStatement(
      s"""SELECT rank_no, $idColumn, event_cnt, buy_cnt
         |FROM $table WHERE window_start = ? ORDER BY rank_no""".stripMargin)
    try {
      statement.setTimestamp(1, windowStart)
      val result = statement.executeQuery()
      val rows = ArrayBuffer.empty[(Int, Long, Long, Long)]
      while (result.next()) {
        rows += ((
          result.getInt("rank_no"),
          result.getLong(idColumn),
          result.getLong("event_cnt"),
          result.getLong("buy_cnt")))
      }
      rows.toSeq
    } finally {
      statement.close()
    }
  }

  private def renderSnapshot(
      metrics: (Timestamp, Timestamp, Long, Long, Long, Long, Long, Long, Double, String, String),
      categories: Seq[(Int, Long, Long, Long)],
      items: Seq[(Int, Long, Long, Long)]): String = {
    val categoryJson = categories.map(renderTopRow("category_id", _)).mkString(",")
    val itemJson = items.map(renderTopRow("item_id", _)).mkString(",")
    s"""{"status":"running","updated_at":"${new Timestamp(System.currentTimeMillis())}","metrics":{"window_start":"${metrics._1}","window_end":"${metrics._2}","pv":${metrics._3},"approx_uv":${metrics._4},"fav_cnt":${metrics._5},"cart_cnt":${metrics._6},"buy_cnt":${metrics._7},"buy_uv":${metrics._8},"pv_to_buy_rate":${metrics._9},"alert_type":"${escape(metrics._10)}","alert_message":"${escape(metrics._11)}"},"category_top10":[$categoryJson],"item_top10":[$itemJson]}"""
  }

  private def renderTopRow(idColumn: String, row: (Int, Long, Long, Long)): String = {
    s"""{"rank":${row._1},"$idColumn":${row._2},"event_cnt":${row._3},"buy_cnt":${row._4}}"""
  }

  private def escape(value: String): String = {
    Option(value).getOrElse("")
      .replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
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

  private def objectStatsDdl(table: String, idColumn: String): String = {
    s"""CREATE TABLE IF NOT EXISTS $table (
       |  window_start DATETIME NOT NULL,
       |  window_end DATETIME NOT NULL,
       |  $idColumn BIGINT NOT NULL,
       |  event_cnt BIGINT NOT NULL DEFAULT 0,
       |  buy_cnt BIGINT NOT NULL DEFAULT 0,
       |  PRIMARY KEY (window_start, $idColumn),
       |  KEY idx_${idColumn}_rank (window_start, event_cnt, buy_cnt)
       |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""".stripMargin
  }

  private def topDdl(table: String, idColumn: String): String = {
    s"""CREATE TABLE IF NOT EXISTS $table (
       |  window_start DATETIME NOT NULL,
       |  window_end DATETIME NOT NULL,
       |  rank_no INT NOT NULL,
       |  $idColumn BIGINT NOT NULL,
       |  event_cnt BIGINT NOT NULL,
       |  buy_cnt BIGINT NOT NULL,
       |  PRIMARY KEY (window_start, $idColumn),
       |  UNIQUE KEY uk_window_rank (window_start, rank_no)
       |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""".stripMargin
  }
}
