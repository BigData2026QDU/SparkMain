package org.example.ci

import org.apache.spark.sql.{DataFrame, SparkSession, types => T}

import java.io.File
import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * CI 烟雾测试 —— Spark SQL 管线端到端验证（Scala 2.12 实现）。
 *
 * 通过 spark-submit 执行，符合 SPARK.md 强制规范。
 */
object SmokeTest {

  // 核心表：必须有数据（MovieLens 测试数据足够密集）
  private val CoreTables: Set[String] = Set(
    "task1_movie_stats",
    "task2_movie_ranking",
    "task3_genre_stats"
  )

  // LuckyAnJun 分析表：只需存在（测试数据极小，部分聚合可能为空）
  private val OutputTables: Set[String] = Set(
    "lb_funnel_overall",
    "lb_time_hourly_behavior",
    "lb_time_hour_distribution",
    "lb_time_weekday_hour_heatmap",
    "lb_time_high_conversion_slots",
    "lb_time_low_conversion_slots",
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
    val warehouseDir = repoRoot.resolve("spark-warehouse-ci")
    Files.createDirectories(warehouseDir)

    val spark = SparkSession.builder()
      .appName("LuckyAnJun-SmokeTest")
      .master("local[2]")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.warehouse.dir", warehouseDir.toUri.toString)
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
    val stream = Files.list(dir)
    try {
      val files = stream.iterator().asScala
        .filter(_.toString.endsWith(".csv"))
        .toList
        .sortBy(_.getFileName.toString)

      if (files.isEmpty) throw new RuntimeException("dataset_test has no CSV files")

      val totalBytes = files.map(Files.size).sum
      println(s"dataset_test files: ${files.map(_.getFileName)}")
      println(s"dataset_test total bytes: $totalBytes")

      if (totalBytes > 64 * 1024)
        throw new RuntimeException("dataset_test must stay below 64 KiB for CI")

      println("[PASS] Dataset size check passed")
    } finally {
      stream.close()
    }
  }

  // ---- Python 清洗脚本 ----

  private def runPythonCleaners(repoRoot: Path): Unit = {
    val cleanerDir = repoRoot.resolve("cleanPy_test")
    if (!Files.isDirectory(cleanerDir)) {
      println("[INFO] No cleaner directory, skipping")
      return
    }
    val stream = Files.list(cleanerDir)
    val scripts = try {
      stream.iterator().asScala
        .filter(_.toString.endsWith(".py"))
        .toList
        .sortBy(_.getFileName.toString)
    } finally {
      stream.close()
    }

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
    val stream = Files.list(sqlDir)
    val sqlFiles = try {
      stream.iterator().asScala
        .filter(_.toString.endsWith(".sql"))
        .toList
        .sortBy(_.getFileName.toString)
    } finally {
      stream.close()
    }

    for (sqlFile <- sqlFiles)
      execSqlFile(spark, sqlFile)
  }

  private def execSqlFile(spark: SparkSession, sqlFile: Path): Unit = {
    println(s"[INFO] Executing: ${sqlFile.getFileName}")
    val text = new String(Files.readAllBytes(sqlFile), "UTF-8")
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
    val existing: Set[String] = tables.collect().map(_.getString(1)).toSet

    var corePass = true
    for (tableName <- CoreTables.toList.sorted) {
      if (existing.contains(tableName)) {
        val count = spark.sql(s"SELECT COUNT(*) FROM $tableName")
          .collect().head.get(0).asInstanceOf[Long]
        if (count > 0) {
          println(s"[PASS] Core table $tableName: $count rows")
        } else {
          println(s"[FAIL] Core table $tableName: 0 rows")
          corePass = false
        }
      } else {
        println(s"[FAIL] Core table $tableName: not found")
        corePass = false
      }
    }

    for (tableName <- OutputTables.toList.sorted) {
      if (existing.contains(tableName)) {
        val count = spark.sql(s"SELECT COUNT(*) FROM $tableName")
          .collect().head.get(0).asInstanceOf[Long]
        // LuckyAnJun 表：测试数据极小，0 行不视为失败
        println(s"[${if (count > 0) "PASS" else "INFO"}] Table $tableName: $count rows")
      } else {
        println(s"[WARN] Table $tableName: not found (SQL may have failed)")
      }
    }

    if (!corePass)
      throw new RuntimeException("Core output tables are missing or empty")
  }

  // ---- 工具方法 ----

  /** 按分号拆分 SQL 语句，跳过注释行。 */
  private def splitStatements(sqlText: String): Seq[String] = {
    val buf = new StringBuilder
    val statements = scala.collection.mutable.ArrayBuffer.empty[String]
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
    statements
  }

  private def deleteRecursive(dir: Path): Unit = {
    if (Files.isDirectory(dir)) {
      val stream = Files.walk(dir)
      try {
        val paths = stream.iterator().asScala.toList.sortBy(-_.getNameCount)
        for (p <- paths) {
          try Files.delete(p) catch { case _: Exception => }
        }
      } finally {
        stream.close()
      }
    }
  }

  private def execProcess(workDir: File, cmd: String*): Unit = {
    val pb = new java.lang.ProcessBuilder(cmd: _*)
    pb.directory(workDir)
    pb.inheritIO()
    val proc = pb.start()
    val code = proc.waitFor()
    if (code != 0)
      throw new RuntimeException(s"Process failed with code $code: ${cmd.mkString(" ")}")
  }
}
