package org.bigdata.analysis

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.bigdata.utils.MySQLExportConfig

final case class AnalysisJobConfig(
    inputDir: String,
    outputDir: String,
    outputFormat: String)

object MovieLensAnalysisSupport {
  private val DefaultInputDirs = Seq("cleanedDataset", "dataset")

  private val RatingsSchema = StructType(Seq(
    StructField("userId", IntegerType, nullable = true),
    StructField("movieId", IntegerType, nullable = true),
    StructField("rating", DoubleType, nullable = true),
    StructField("timestamp", LongType, nullable = true)
  ))

  private val MoviesSchema = StructType(Seq(
    StructField("movieId", IntegerType, nullable = true),
    StructField("title", StringType, nullable = true),
    StructField("genres", StringType, nullable = true)
  ))

  def parseArgs(args: Array[String], taskName: String): AnalysisJobConfig = {
    var inputDir: Option[String] = None
    var outputDir: Option[String] = None
    var outputFormat = "parquet"
    val positional = scala.collection.mutable.ArrayBuffer.empty[String]

    var index = 0
    while (index < args.length) {
      args(index) match {
        case "--input" | "--input-dir" =>
          index += 1
          require(index < args.length, s"${args(index - 1)} requires a value")
          inputDir = Some(args(index))
        case "--output" | "--output-dir" =>
          index += 1
          require(index < args.length, s"${args(index - 1)} requires a value")
          outputDir = Some(args(index))
        case "--format" =>
          index += 1
          require(index < args.length, "--format requires a value")
          outputFormat = args(index).trim.toLowerCase
        case "--help" | "-h" =>
          printTaskUsage(taskName)
          sys.exit(0)
        case unknown if unknown.startsWith("--") =>
          throw new IllegalArgumentException(s"unknown argument: $unknown")
        case value =>
          positional += value
      }
      index += 1
    }

    AnalysisJobConfig(
      inputDir = inputDir
        .orElse(positional.headOption)
        .getOrElse(defaultInputDir()),
      outputDir = outputDir
        .orElse(positional.drop(1).headOption)
        .getOrElse(s"output/$taskName"),
      outputFormat = outputFormat)
  }

  def spark(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .getOrCreate()
  }

  def readRatings(spark: SparkSession, inputDir: String): DataFrame = {
    spark.read
      .schema(RatingsSchema)
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(resolveCsv(inputDir, "ratings.csv"))
      .na.drop(Seq("userId", "movieId", "rating", "timestamp"))
  }

  def readMovies(spark: SparkSession, inputDir: String): DataFrame = {
    spark.read
      .schema(MoviesSchema)
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(resolveCsv(inputDir, "movies.csv"))
      .na.drop(Seq("movieId", "title", "genres"))
  }

  def writeReport(dataFrame: DataFrame, config: AnalysisJobConfig): Unit = {
    config.outputFormat match {
      case "parquet" =>
        dataFrame.write.mode(SaveMode.Overwrite).parquet(config.outputDir)
      case "csv" =>
        dataFrame.write
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv(config.outputDir)
      case other =>
        throw new IllegalArgumentException(
          s"unsupported output format '$other'; use parquet or csv")
    }
  }

  def exportReportIfConfigured(
      dataFrame: DataFrame,
      tableEnvVar: String): Unit = {
    val exportConfig =
      MySQLExportConfig.fromEnvironment(
        Seq(tableEnvVar),
        enabledEnvVar = "OFFLINE_MYSQL_ENABLED")
    val exported = MySQLExportConfig.exportIfEnabled(
      dataFrame,
      exportConfig,
      SaveMode.Overwrite)

    if (exported) {
      println(
        s"MySQL export enabled for table ${MySQLExportConfig.tableName(exportConfig).getOrElse("<configured>")}")
    } else {
      println("MySQL export disabled")
    }
  }

  private def defaultInputDir(): String = {
    DefaultInputDirs.find(dir => Files.isDirectory(Paths.get(dir))).getOrElse("dataset")
  }

  private def resolveCsv(inputDir: String, fileName: String): String = {
    val candidates = (Seq(inputDir) ++ DefaultInputDirs).distinct
      .map(dir => Paths.get(dir, fileName))

    candidates.find(Files.isRegularFile(_)).map(_.toString).getOrElse {
      throw new IllegalArgumentException(
        s"cannot find $fileName under ${candidates.map(_.toString).mkString(", ")}")
    }
  }

  private def printTaskUsage(taskName: String): Unit = {
    println(s"Usage: $taskName [inputDir] [outputDir] [--format parquet|csv]")
    println(s"Default input: cleanedDataset if present, otherwise dataset")
    println(s"Default output: output/$taskName")
  }
}
