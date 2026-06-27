package org.bigdata.streaming

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class RealtimeWorkflowSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var warehouse: Path = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    warehouse = Files.createTempDirectory("spark-warehouse-")
    spark = SparkSession.builder()
      .appName("RealtimeWorkflowSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
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

  test("structured streaming aggregates realtime ratings without external services") {
    val inputDirectory = Files.createTempDirectory("ratings-input-")
    val checkpointDirectory = Files.createTempDirectory("ratings-checkpoint-")
    val queryName = s"realtime_stats_${System.nanoTime()}"
    val inputSchema = StructType(Seq(StructField("json", StringType, nullable = false)))

    val input = spark.readStream
      .schema(inputSchema)
      .json(inputDirectory.toString)

    val stats = RealtimeWorkflow.aggregateRatings(
      RealtimeWorkflow.parseRatings(input))

    val records = Seq(
      """{"json":"{\"userId\":1,\"movieId\":10,\"rating\":5.0,\"timestamp\":1704067200}"}""",
      """{"json":"{\"userId\":2,\"movieId\":10,\"rating\":3.0,\"timestamp\":1704067210}"}""",
      """{"json":"{\"userId\":3,\"movieId\":20,\"rating\":4.0,\"timestamp\":1704067220}"}"""
    )
    Files.write(
      inputDirectory.resolve("ratings.json"),
      records.asJava,
      StandardCharsets.UTF_8)

    val query = stats.writeStream
      .format("memory")
      .queryName(queryName)
      .outputMode("update")
      .option("checkpointLocation", checkpointDirectory.toString)
      .trigger(Trigger.AvailableNow())
      .start()

    assert(query.awaitTermination(60000), "streaming query did not finish")

    val rows = spark.table(queryName)
      .select("movieId", "rating_count", "avg_rating", "max_rating", "min_rating")
      .collect()
      .map(row => row.getInt(0) -> row)
      .toMap

    assert(rows.keySet == Set(10, 20))
    assert(rows(10).getLong(1) == 2L)
    assert(rows(10).getDouble(2) == 4.0)
    assert(rows(10).getDouble(3) == 5.0)
    assert(rows(10).getDouble(4) == 3.0)
    assert(rows(20).getLong(1) == 1L)
  }

  test("test mode rejects production MySQL resources") {
    val error = intercept[IllegalArgumentException] {
      RealtimeConfig.fromEnvironment(Map(
        "SPARKMAIN_TEST_MODE" -> "true",
        "MYSQL_ENABLED" -> "true",
        "MYSQL_JDBC_URL" -> "jdbc:mysql://localhost:3306/bigdata_ana",
        "MYSQL_TABLE" -> "realtime_stats"
      ))
    }

    assert(error.getMessage.contains("production MySQL"))
  }

  test("test mode permits an isolated in-memory database") {
    val config = RealtimeConfig.fromEnvironment(Map(
      "SPARKMAIN_TEST_MODE" -> "true",
      "MYSQL_ENABLED" -> "true",
      "MYSQL_JDBC_URL" -> "jdbc:h2:mem:sparkmain_test",
      "MYSQL_TABLE" -> "realtime_stats_test"
    ))

    assert(config.mysqlEnabled)
    assert(config.mysqlTable == "realtime_stats_test")
  }
}
