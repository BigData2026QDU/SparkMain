package org.bigdata.analysis

import java.nio.file.{Files, Path}

import org.apache.spark.sql.SparkSession
import org.bigdata.utils.MySQLExportConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class AnalysisReportsSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var warehouse: Path = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    warehouse = Files.createTempDirectory("analysis-warehouse-")
    spark = SparkSession.builder()
      .appName("AnalysisReportsSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.warehouse.dir", warehouse.toUri.toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override protected def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
      }
    } finally {
      super.afterAll()
    }
  }

  test("AnalyzeRatings builds a movie quality report") {
    val session = spark
    import session.implicits._

    val ratings = Seq(
      (1, 10, 5.0, 1704067200L),
      (2, 10, 3.0, 1704153600L),
      (3, 20, 4.0, 1704240000L),
      (1, 30, 2.0, 1704326400L)
    ).toDF("userId", "movieId", "rating", "timestamp")

    val movies = Seq(
      (10, "Alpha", "Action|Drama"),
      (20, "Beta", "Comedy"),
      (30, "Gamma", "Action")
    ).toDF("movieId", "title", "genres")

    val rows = AnalyzeRatings.buildReport(ratings, movies)
      .select("movieId", "title", "rating_count", "avg_rating")
      .collect()
      .map(row => row.getInt(0) -> row)
      .toMap

    assert(rows(10).getString(1) == "Alpha")
    assert(rows(10).getLong(2) == 2L)
    assert(rows(10).getDouble(3) == 4.0)
    assert(rows(30).getDouble(3) == 2.0)
  }

  test("AnalyzeGenres explodes genres and reports category popularity") {
    val session = spark
    import session.implicits._

    val ratings = Seq(
      (1, 10, 5.0, 1704067200L),
      (2, 10, 3.0, 1704153600L),
      (3, 20, 4.0, 1704240000L),
      (1, 30, 2.0, 1704326400L)
    ).toDF("userId", "movieId", "rating", "timestamp")

    val movies = Seq(
      (10, "Alpha", "Action|Drama"),
      (20, "Beta", "Comedy"),
      (30, "Gamma", "Action")
    ).toDF("movieId", "title", "genres")

    val rows = AnalyzeGenres.buildReport(ratings, movies)
      .select("genre", "rating_count", "movie_count", "rating_share")
      .collect()
      .map(row => row.getString(0) -> row)
      .toMap

    assert(rows("Action").getLong(1) == 3L)
    assert(rows("Action").getLong(2) == 2L)
    assert(rows("Action").getDouble(3) == 0.75)
    assert(rows("Comedy").getLong(1) == 1L)
  }

  test("AnalyzeTime aggregates rating trends by month") {
    val session = spark
    import session.implicits._

    val ratings = Seq(
      (1, 10, 5.0, 1704067200L),
      (2, 10, 3.0, 1704153600L),
      (3, 20, 4.0, 1706745600L)
    ).toDF("userId", "movieId", "rating", "timestamp")

    val rows = AnalyzeTime.buildReport(ratings)
      .select("rating_month", "rating_count", "active_users", "avg_rating")
      .collect()
      .map(row => row.getString(0) -> row)
      .toMap

    assert(rows("2024-01").getLong(1) == 2L)
    assert(rows("2024-01").getLong(2) == 2L)
    assert(rows("2024-01").getDouble(3) == 4.0)
    assert(rows("2024-02").getLong(1) == 1L)
  }

  test("AnalyzeUsers summarizes user behavior segments") {
    val session = spark
    import session.implicits._

    val ratings = Seq(
      (1, 10, 5.0, 1704067200L),
      (1, 20, 3.0, 1704153600L),
      (2, 10, 3.0, 1704240000L),
      (3, 30, 4.0, 1704326400L)
    ).toDF("userId", "movieId", "rating", "timestamp")

    val report = AnalyzeUsers.buildReport(ratings)

    assert(report.columns.contains("activity_segment"))
    assert(report.columns.contains("rating_tendency"))
    assert(report.agg(org.apache.spark.sql.functions.sum("user_count")).first().getLong(0) == 3L)
  }

  test("offline MySQL export ignores generic realtime enable and table variables") {
    val config = MySQLExportConfig.fromEnvironment(
      Seq("MYSQL_TABLE_RATINGS"),
      Map(
        "MYSQL_ENABLED" -> "true",
        "MYSQL_JDBC_URL" -> "jdbc:mysql://example.invalid:3306/app",
        "MYSQL_USER" -> "user",
        "MYSQL_PASSWORD" -> "password",
        "MYSQL_TABLE" -> "shared_table"),
      enabledEnvVar = "OFFLINE_MYSQL_ENABLED")

    assert(!config.enabled)
  }

  test("offline MySQL export requires the task-specific table variable") {
    val error = intercept[IllegalArgumentException] {
      MySQLExportConfig.fromEnvironment(
        Seq("MYSQL_TABLE_RATINGS"),
        Map(
          "OFFLINE_MYSQL_ENABLED" -> "true",
          "MYSQL_JDBC_URL" -> "jdbc:mysql://example.invalid:3306/app",
          "MYSQL_USER" -> "user",
          "MYSQL_PASSWORD" -> "password",
          "MYSQL_TABLE" -> "shared_table"),
        enabledEnvVar = "OFFLINE_MYSQL_ENABLED")
    }

    assert(error.getMessage.contains("MYSQL_TABLE_RATINGS"))
    assert(error.getMessage.contains("OFFLINE_MYSQL_ENABLED=true"))
  }

  test("offline MySQL export accepts a task-specific table variable") {
    val config = MySQLExportConfig.fromEnvironment(
      Seq("MYSQL_TABLE_GENRES"),
      Map(
        "OFFLINE_MYSQL_ENABLED" -> "true",
        "MYSQL_JDBC_URL" -> "jdbc:mysql://example.invalid:3306/app",
        "MYSQL_USER" -> "user",
        "MYSQL_PASSWORD" -> "password",
        "MYSQL_TABLE_GENRES" -> "genres_report"),
      enabledEnvVar = "OFFLINE_MYSQL_ENABLED")

    assert(config.enabled)
    assert(MySQLExportConfig.tableName(config).contains("genres_report"))
  }
}
