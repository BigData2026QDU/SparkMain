package org.example.ci

import org.apache.spark.sql.{DataFrame, Row, SparkSession, types => T}

import java.io.File
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}
import sys.process._

/**
 * CI 烟雾测试 —— Spark SQL 管线端到端验证（Scala 实现）。
 *
 * 通过 spark-submit 执行，符合 SPARK.md 强制规范。
 */
object SmokeTest {

  private val OutputTables: Set[String] = Set(
    "task1_movie_stats",
    "task2_movie_ranking",
    "task3_genre_stats",
    "lb_funnel_overall",
    "lb_time_hourly_behavior",
    "lb_category_efficiency",
    "lb_item_efficiency",
    "lb_user_segment_summary"
  )

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: SmokeTest <repo-root>")
      System.exit(2)
    }
    val repoRoot = Paths.get(args(0)).toAbsolutePath.normalize()
    if (!Files.isDirectory(repoRoot)) {
      System.err.println(s"Invalid repo root: $repoRoot")
      System.exit(2)
    }

    val spark = initSpark(repoRoot)
    try {
      verifyDatasetSize(repoRoot)
      runPythonCleaners(repoRoot)
      loadCsvData(spark, repoRoot)
      execSqlDir(spark, repoRoot, "prepareData_test")
      execSqlDir(spark, repoRoot, "jobSQL_test")
      verifyOutputTables(spark)
      println("\n[SUCCESS] All Spark SQL smoke tests passed!")
    } finally {
      spark.stop()
    }
  }

  // ---- Spark 初始化 ----

  private def initSpark(repoRoot: Path): SparkSession = {
    val spark = SparkSession.builder()
      .appName("LuckyAnJun-SmokeTest")
      .master("local[2]")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.warehouse.dir", repoRoot.resolve("spark-warehouse-ci").toString)
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE DATABASE IF NOT EXISTS bigdata_ana_test")
    spark.sql("USE bigdata_ana_test")
    println("[INFO] Spark session started (local[2], Hive enabled)")
    spark
  }

  // ---- 数据集大小校验 ----

  private def verifyDatasetSize(repoRoot: Path): Unit = {
    val dir = repoRoot.resolve("dataset_test")
    val files = Using(Files.list(dir)) { stream =>
      stream.iterator().asScala
        .filter(_.toString.endsWith(".csv"))
        .toList
        .sortBy(_.getFileName.toString)
    }.get

    if (files.isEmpty) throw new RuntimeException("dataset_test has no CSV files")

    val totalBytes = files.map(Files.size).sum
    println(s"dataset_test files: ${files.map(_.getFileName)}")
    println(s"dataset_test total bytes: $totalBytes")

    if (totalBytes > 64 * 1024)
      throw new RuntimeException("dataset_test must stay below 64 KiB for CI")

    println("[PASS] Dataset size check passed")
  }

  // ---- Python 清洗脚本 ----

  private def runPythonCleaners(repoRoot: Path): Unit = {
    val cleanerDir = repoRoot.resolve("cleanPy_test")
    if (!Files.isDirectory(cleanerDir)) {
      println("[INFO] No cleaner directory, skipping")
      return
    }
    val scripts = Using(Files.list(cleanerDir)) { stream =>
      stream.iterator().asScala
        .filter(_.toString.endsWith(".py"))
        .toList
        .sortBy(_.getFileName.toString)
    }.get

    if (scripts.isEmpty) {
      println("[INFO] No cleaner scripts found, skipping")
      return
    }

    val cleanedDir = repoRoot.resolve("cleanedDataset_test")
    deleteRecursive(cleanedDir)
    Files.createDirectories(cleanedDir)

    for (script <- scripts) {
      println(s"[INFO] Running cleaner: ${script.getFileName}")
      execProcess(repoRoot.toFile, "python", script.toString)
    }
  }

  // ---- CSV 数据加载 ----

  private def loadCsvData(spark: SparkSession, repoRoot: Path): Unit = {
    val datasetDir = repoRoot.resolve("dataset_test")
    val cleanedDir = repoRoot.resolve("cleanedDataset_test")

    val moviesSchema = T.StructType(Seq(
      T.StructField("movieId", T.IntegerType),
      T.StructField("title", T.StringType),
      T.StructField("genres", T.StringType)))
    readCsv(spark, datasetDir.resolve("movies.csv"), moviesSchema)
      .createOrReplaceTempView("movies")

    val ratingsSchema = T.StructType(Seq(
      T.StructField("userId", T.IntegerType),
      T.StructField("movieId", T.IntegerType),
      T.StructField("rating", T.DoubleType),
      T.StructField("timestamp", T.LongType)))
    readCsv(spark, datasetDir.resolve("ratings.csv"), ratingsSchema)
      .createOrReplaceTempView("ratings")

    val tagsSchema = T.StructType(Seq(
      T.StructField("userId", T.IntegerType),
      T.StructField("movieId", T.IntegerType),
      T.StructField("tag", T.StringType),
      T.StructField("timestamp", T.LongType)))
    readCsv(spark, datasetDir.resolve("tags.csv"), tagsSchema)
      .createOrReplaceTempView("tags")

    val linksSchema = T.StructType(Seq(
      T.StructField("movieId", T.IntegerType),
      T.StructField("imdbId", T.StringType),
      T.StructField("tmdbId", T.IntegerType)))
    readCsv(spark, datasetDir.resolve("links.csv"), linksSchema)
      .createOrReplaceTempView("links")

    // 清洗后的 user_behavior
    val ub = cleanedDir.resolve("user_behavior.csv")
    if (!Files.isRegularFile(ub))
      throw new RuntimeException(s"Missing cleaned user_behavior: $ub")
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv(ub.toString)
      .createOrReplaceTempView("user_behavior")

    println("[INFO] All CSV data loaded as temp views")
  }

  private def readCsv(spark: SparkSession, path: Path, schema: T.StructType): DataFrame = {
    if (!Files.isRegularFile(path))
      throw new RuntimeException(s"Missing test CSV: $path")
    spark.read.option("header", "true").schema(schema).csv(path.toString)
  }

  // ---- SQL 执行 ----

  private def execSqlDir(spark: SparkSession, repoRoot: Path, dirName: String): Unit = {
    val sqlDir = repoRoot.resolve(dirName)
    if (!Files.isDirectory(sqlDir)) {
      println(s"[WARN] SQL directory not found: $dirName")
      return
    }
    println(s"\n[STEP] Running $dirName SQL...")
    val sqlFiles = Using(Files.list(sqlDir)) { stream =>
      stream.iterator().asScala
        .filter(_.toString.endsWith(".sql"))
        .toList
        .sortBy(_.getFileName.toString)
    }.get

    for (sqlFile <- sqlFiles)
      execSqlFile(spark, sqlFile)
  }

  private def execSqlFile(spark: SparkSession, sqlFile: Path): Unit = {
    println(s"[INFO] Executing: ${sqlFile.getFileName}")
    val text = Files.readString(sqlFile)
    for (stmt <- splitStatements(text)) {
      val trimmed = stmt.trim
      if (trimmed.nonEmpty) {
        val upper = trimmed.toUpperCase
        if (upper.startsWith("SET ") || upper == "USE BIGDATA_ANA_TEST") {
          // CI 已配置
        } else {
          Try(spark.sql(trimmed)).recover {
            case e: Exception =>
              if (upper.startsWith("DROP "))
                println("  [OK] (table may not exist)")
              else
                println(s"  [WARN] ${sqlFile.getFileName}: ${e.getMessage.take(120)}")
          }
        }
      }
    }
  }

  // ---- 结果验证 ----

  private def verifyOutputTables(spark: SparkSession): Unit = {
    println("\n[VERIFY] Checking output tables...")
    val tables = spark.sql("SHOW TABLES")
    val existing = tables.collectAsList().asScala.map(_.getString(1)).toSet

    var allPass = true
    for (tableName <- OutputTables.toList.sorted) {
      if (existing.contains(tableName)) {
        val count = spark.sql(s"SELECT COUNT(*) FROM $tableName")
          .collectAsList().get(0).get(0)
        if (count.asInstanceOf[Long] > 0) {
          println(s"[PASS] Table $tableName: $count rows")
        } else {
          println(s"[FAIL] Table $tableName: 0 rows")
          allPass = false
        }
      } else {
        println(s"[FAIL] Table $tableName: not found")
        allPass = false
      }
    }

    if (!allPass)
      throw new RuntimeException("Some expected output tables are missing or empty")
  }

  // ---- 工具方法 ----

  /** 按分号拆分 SQL 语句，跳过注释行。 */
  private def splitStatements(sqlText: String): Seq[String] = {
    val buf = new StringBuilder
    val statements = Seq.newBuilder[String]
    for (line <- sqlText.split("\n")) {
      val stripped = line.strip
      if (!stripped.startsWith("--")) {
        buf.append(line).append('\n')
        if (stripped.endsWith(";")) {
          var stmt = buf.toString().strip
          if (stmt.endsWith(";")) stmt = stmt.dropRight(1)
          statements += stmt
          buf.clear()
        }
      }
    }
    val remaining = buf.toString().strip
    if (remaining.nonEmpty) statements += remaining
    statements.result()
  }

  private def deleteRecursive(dir: Path): Unit = {
    if (Files.isDirectory(dir)) {
      Using(Files.walk(dir)) { stream =>
        stream.iterator().asScala.toList.sortBy(-_.getNameCount).foreach { p =>
          try Files.delete(p) catch { case _: Exception => }
        }
      }
    }
  }

  private def execProcess(workDir: File, cmd: String*): Unit = {
    val proc = new ProcessBuilder(cmd: _*)
      .directory(workDir)
      .inheritIO()
      .start()
    val code = proc.waitFor()
    if (code != 0)
      throw new RuntimeException(s"Process failed with code $code: ${cmd.mkString(" ")}")
  }
}
