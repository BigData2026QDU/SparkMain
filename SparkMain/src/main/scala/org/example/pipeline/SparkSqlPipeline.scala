package org.example.pipeline

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object SparkSqlPipeline {
  private val SourceSchemas: Map[String, StructType] = Map(
    "movies" -> StructType(
      Seq(
        StructField("movieId", IntegerType, nullable = false),
        StructField("title", StringType, nullable = false),
        StructField("genres", StringType, nullable = false)
      )
    ),
    "ratings" -> StructType(
      Seq(
        StructField("userId", IntegerType, nullable = false),
        StructField("movieId", IntegerType, nullable = false),
        StructField("rating", DoubleType, nullable = false),
        StructField("timestamp", LongType, nullable = false)
      )
    ),
    "tags" -> StructType(
      Seq(
        StructField("userId", IntegerType, nullable = false),
        StructField("movieId", IntegerType, nullable = false),
        StructField("tag", StringType, nullable = false),
        StructField("timestamp", LongType, nullable = false)
      )
    ),
    "links" -> StructType(
      Seq(
        StructField("movieId", IntegerType, nullable = false),
        StructField("imdbId", StringType, nullable = false),
        StructField("tmdbId", IntegerType, nullable = true)
      )
    )
  )

  private val ExpectedSmokeObjects = Seq(
    "movies",
    "ratings",
    "tags",
    "links",
    "task1_movie_stats",
    "task2_movie_ranking",
    "task3_genre_stats",
    "v_movies_ratings"
  )

  final case class Config(
      database: String = "bigdata_ana",
      datasetDir: Path = Paths.get("dataset"),
      cleanedDir: Path = Paths.get("cleanedDataset"),
      prepareDir: Path = Paths.get("prepareData"),
      jobDir: Path = Paths.get("jobSQL"),
      warehouseDir: Path = Paths.get("spark-warehouse"),
      outputDir: Path = Paths.get("output", "bigdata_ana"),
      validate: Boolean = false,
      exportResults: Boolean = true,
      cleanDatabase: Boolean = true
  )

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args.toList)
    val spark = SparkSession
      .builder()
      .appName(s"SparkMain SQL Pipeline (${config.database})")
      .config("spark.sql.warehouse.dir", config.warehouseDir.toAbsolutePath.toUri.toString)
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.legacy.createHiveTableByDefault", "false")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    try {
      run(config, spark)
    } finally {
      spark.stop()
    }
  }

  def run(config: Config, spark: SparkSession): Unit = {
    if (config.cleanDatabase) {
      spark.sql(s"DROP DATABASE IF EXISTS ${quoteIdent(config.database)} CASCADE")
    }
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${quoteIdent(config.database)}")
    spark.sql(s"USE ${quoteIdent(config.database)}")

    loadSourceTables(spark, config)
    runSqlDirectory(spark, config.prepareDir)
    runSqlDirectory(spark, config.jobDir)

    if (config.validate) {
      validateSmokeResults(spark)
    }
    if (config.exportResults) {
      exportResultTables(spark, config.outputDir)
    }
    println("[SUCCESS] Spark SQL pipeline completed")
  }

  private def loadSourceTables(spark: SparkSession, config: Config): Unit = {
    SourceSchemas.toSeq.sortBy(_._1).foreach { case (tableName, schema) =>
      val csvPath = sourceCsvPath(config, tableName)
      val df = spark.read
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(csvPath.toString)

      spark.sql(s"DROP TABLE IF EXISTS ${quoteIdent(tableName)}")
      df.write.mode("overwrite").saveAsTable(tableName)

      val rowCount = spark.table(tableName).count()
      require(rowCount > 0, s"Source table $tableName loaded zero rows from $csvPath")
      println(s"[PASS] Loaded $tableName from $csvPath ($rowCount rows)")
    }
  }

  private def sourceCsvPath(config: Config, tableName: String): Path = {
    val cleanedPath = config.cleanedDir.resolve(s"$tableName.csv")
    val datasetPath = config.datasetDir.resolve(s"$tableName.csv")
    if (Files.exists(cleanedPath)) cleanedPath
    else if (Files.exists(datasetPath)) datasetPath
    else throw new IllegalArgumentException(s"Missing CSV for $tableName in ${config.cleanedDir} or ${config.datasetDir}")
  }

  private def runSqlDirectory(spark: SparkSession, dir: Path): Unit = {
    if (!Files.isDirectory(dir)) {
      println(s"[INFO] SQL directory not found, skipping: $dir")
      return
    }

    val files = Files.list(dir).iterator().asScala
      .filter(path => Files.isRegularFile(path) && path.getFileName.toString.endsWith(".sql"))
      .toSeq
      .sortBy(_.getFileName.toString)

    files.foreach { sqlFile =>
      println(s"[INFO] Running SQL file: $sqlFile")
      splitSql(new String(Files.readAllBytes(sqlFile), StandardCharsets.UTF_8)).foreach { statement =>
        if (isSkippedLegacySetting(statement)) {
          println(s"[INFO] Skipping legacy setting: ${firstLine(statement)}")
        } else {
          println(s"[SQL] ${firstLine(statement).take(120)}")
          val result = spark.sql(statement)
          printSmallResult(result)
        }
      }
    }
  }

  private def splitSql(sqlText: String): Seq[String] = {
    val statements = ListBuffer.empty[String]
    val current = new StringBuilder

    sqlText.linesIterator.foreach { rawLine =>
      val cleanLine = rawLine.stripPrefix("\uFEFF")
      val line = cleanLine.trim
      if (line.nonEmpty && !line.startsWith("--")) {
        current.append(cleanLine).append('\n')
        if (line.endsWith(";")) {
          val statement = current.toString().trim.stripSuffix(";").trim
          if (statement.nonEmpty) {
            statements += statement
          }
          current.clear()
        }
      }
    }

    val tail = current.toString().trim
    if (tail.nonEmpty) {
      statements += tail
    }
    statements.toSeq
  }

  private def isSkippedLegacySetting(statement: String): Boolean = {
    val normalized = statement.trim.toLowerCase
    normalized.startsWith("set hive.") || normalized == "set spark.master=local[*]"
  }

  private def firstLine(statement: String): String = statement.split("\\R", 2).head.trim

  private def printSmallResult(df: DataFrame): Unit = {
    if (df.schema.nonEmpty) {
      df.show(20, truncate = false)
    }
  }

  private def validateSmokeResults(spark: SparkSession): Unit = {
    ExpectedSmokeObjects.foreach { tableName =>
      require(spark.catalog.tableExists(tableName), s"Missing table or view: $tableName")
      val rowCount = spark.table(tableName).count()
      require(rowCount > 0, s"$tableName has no rows")
      println(s"[PASS] $tableName has $rowCount rows")
    }
  }

  private def exportResultTables(spark: SparkSession, outputDir: Path): Unit = {
    val outputRoot = outputDir.toAbsolutePath
    Files.createDirectories(outputRoot)

    val resultTables = spark.catalog.listTables().collect()
      .map(_.name)
      .filter(_.startsWith("task"))
      .sorted

    resultTables.foreach { tableName =>
      val target = outputRoot.resolve(tableName)
      spark.table(tableName)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(target.toString)
      println(s"[INFO] Exported $tableName to $target")
    }
  }

  private def quoteIdent(identifier: String): String = {
    require(identifier.matches("[A-Za-z_][A-Za-z0-9_]*"), s"Unsafe identifier: $identifier")
    s"`$identifier`"
  }

  private def parseArgs(args: List[String]): Config = {
    def nextValue(flag: String, rest: List[String]): String = rest match {
      case value :: _ => value
      case Nil => throw new IllegalArgumentException(s"Missing value for $flag")
    }

    def loop(remaining: List[String], config: Config): Config = remaining match {
      case Nil => config
      case "--database" :: tail => loop(tail.drop(1), config.copy(database = nextValue("--database", tail)))
      case "--dataset-dir" :: tail => loop(tail.drop(1), config.copy(datasetDir = Paths.get(nextValue("--dataset-dir", tail))))
      case "--cleaned-dir" :: tail => loop(tail.drop(1), config.copy(cleanedDir = Paths.get(nextValue("--cleaned-dir", tail))))
      case "--prepare-dir" :: tail => loop(tail.drop(1), config.copy(prepareDir = Paths.get(nextValue("--prepare-dir", tail))))
      case "--job-dir" :: tail => loop(tail.drop(1), config.copy(jobDir = Paths.get(nextValue("--job-dir", tail))))
      case "--warehouse-dir" :: tail => loop(tail.drop(1), config.copy(warehouseDir = Paths.get(nextValue("--warehouse-dir", tail))))
      case "--output-dir" :: tail => loop(tail.drop(1), config.copy(outputDir = Paths.get(nextValue("--output-dir", tail))))
      case "--validate" :: tail => loop(tail, config.copy(validate = true))
      case "--no-export" :: tail => loop(tail, config.copy(exportResults = false))
      case "--keep-database" :: tail => loop(tail, config.copy(cleanDatabase = false))
      case unknown :: _ => throw new IllegalArgumentException(s"Unknown argument: $unknown")
    }

    loop(args, Config())
  }
}
