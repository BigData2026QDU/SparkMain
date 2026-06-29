package org.bigdata.export

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.{SaveMode, SparkSession}

object UserBehaviorMySQLExportJob {

  private val DefaultTables = Seq(
    "lb_funnel_item_path_stage",
    "lb_time_hour_distribution",
    "lb_category_topn",
    "lb_user_active_day_distribution")

  def main(args: Array[String]): Unit = {
    val sourceDatabase = arg(args, 0, "bigdata_ana")
    val jdbcUrl = arg(args, 1, env(
      "MYSQL_JDBC_URL",
      "jdbc:mysql://127.0.0.1:13306/test_db?useUnicode=true&characterEncoding=utf8&useSSL=false"))
    val mysqlUser = arg(args, 2, env("MYSQL_USER", "test"))
    val mysqlPassword = env("MYSQL_PASSWORD", "")
    val tables = env("MYSQL_EXPORT_TABLES", DefaultTables.mkString(","))
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toSeq

    require(mysqlPassword.nonEmpty, "MYSQL_PASSWORD must be set")
    require(tables.nonEmpty, "No export tables configured")
    tables.foreach(validateIdentifier)

    val spark = SparkSession.builder()
      .appName("LuckyAnJunHiveToMySQLExport")
      .config("spark.sql.session.timeZone", "Asia/Shanghai")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val properties = new Properties()
    properties.setProperty("user", mysqlUser)
    properties.setProperty("password", mysqlPassword)
    properties.setProperty("driver", mysqlDriver)

    try {
      tables.foreach { table =>
        exportTable(spark, sourceDatabase, table, jdbcUrl, properties)
      }
      println("[SUCCESS] Exported " + tables.length + " Hive result tables to MySQL")
    } finally {
      spark.stop()
    }
  }

  private def exportTable(
      spark: SparkSession,
      sourceDatabase: String,
      table: String,
      jdbcUrl: String,
      properties: Properties): Unit = {
    val sourceTable = sourceDatabase + "." + table
    require(spark.catalog.tableExists(sourceDatabase, table), "Hive table not found: " + sourceTable)

    val source = spark.table(sourceTable)
      .withColumn("spark_exported_at", current_timestamp())
      .cache()
    val sourceCount = source.count()
    val stagingTable = table + "__staging"

    try {
      source.coalesce(if (sourceCount >= 100000L) 4 else 1)
        .write
        .mode(SaveMode.Overwrite)
        .option("batchsize", "2000")
        .option("isolationLevel", "NONE")
        .jdbc(jdbcUrl, stagingTable, properties)

      withConnection(jdbcUrl, properties) { connection =>
        val stagingCount = countRows(connection, stagingTable)
        require(
          stagingCount == sourceCount,
          s"Row count mismatch for $table: Hive=$sourceCount, MySQL staging=$stagingCount")
        promote(connection, stagingTable, table)
        val targetCount = countRows(connection, table)
        require(
          targetCount == sourceCount,
          s"Published row count mismatch for $table: Hive=$sourceCount, MySQL=$targetCount")
      }

      println("[EXPORTED] " + table + ": " + sourceCount + " rows")
    } finally {
      source.unpersist()
    }
  }

  private def promote(connection: Connection, stagingTable: String, targetTable: String): Unit = {
    val statement = connection.createStatement()
    val backupTable = targetTable + "__backup"
    try {
      statement.executeUpdate("DROP TABLE IF EXISTS " + quote(backupTable))
      if (tableExists(connection, targetTable)) {
        statement.executeUpdate(
          "RENAME TABLE " + quote(targetTable) + " TO " + quote(backupTable) +
            ", " + quote(stagingTable) + " TO " + quote(targetTable))
        statement.executeUpdate("DROP TABLE " + quote(backupTable))
      } else {
        statement.executeUpdate(
          "RENAME TABLE " + quote(stagingTable) + " TO " + quote(targetTable))
      }
    } finally {
      statement.close()
    }
  }

  private def tableExists(connection: Connection, table: String): Boolean = {
    val result = connection.getMetaData.getTables(connection.getCatalog, null, table, Array("TABLE"))
    try result.next() finally result.close()
  }

  private def countRows(connection: Connection, table: String): Long = {
    val statement = connection.createStatement()
    try {
      val result = statement.executeQuery("SELECT COUNT(*) FROM " + quote(table))
      try {
        result.next()
        result.getLong(1)
      } finally {
        result.close()
      }
    } finally {
      statement.close()
    }
  }

  private def withConnection(
      jdbcUrl: String,
      properties: Properties)(operation: Connection => Unit): Unit = {
    val connection = DriverManager.getConnection(jdbcUrl, properties)
    try operation(connection) finally connection.close()
  }

  private def mysqlDriver: String = {
    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      "com.mysql.cj.jdbc.Driver"
    } catch {
      case _: ClassNotFoundException =>
        Class.forName("com.mysql.jdbc.Driver")
        "com.mysql.jdbc.Driver"
    }
  }

  private def validateIdentifier(identifier: String): Unit = {
    require(
      identifier.matches("[A-Za-z0-9_]+"),
      "Unsafe MySQL identifier: " + identifier)
  }

  private def quote(identifier: String): String = {
    validateIdentifier(identifier)
    "`" + identifier + "`"
  }

  private def arg(args: Array[String], index: Int, defaultValue: String): String = {
    if (args.length > index && args(index).nonEmpty) args(index) else defaultValue
  }

  private def env(name: String, defaultValue: String): String = {
    sys.env.get(name).filter(_.nonEmpty).getOrElse(defaultValue)
  }
}
