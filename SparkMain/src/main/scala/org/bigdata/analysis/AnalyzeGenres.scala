package org.bigdata.analysis

import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.bigdata.utils.MySQLExporter
import java.util.Properties

/**
 * 类型分析任务
 * 分析各类型电影的评分分布，输出到 MySQL
 */
object AnalyzeGenres {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AnalyzeGenres")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    // 读取电影数据
    val moviesPath = if (args.length > 0) args(0) else "dataset/movies.csv"
    val movies = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(moviesPath)

    // 读取评分数据
    val ratingsPath = if (args.length > 1) args(1) else "output/ratings_streaming"
    val ratings = spark.read.parquet(ratingsPath)

    // 创建临时视图
    movies.createOrReplaceTempView("movies")
    ratings.createOrReplaceTempView("ratings")

    // 分析: 各类型电影的平均评分
    val genreStats = spark.sql("""
      SELECT 
        genre,
        COUNT(DISTINCT m.movieId) as movie_count,
        ROUND(AVG(r.rating), 2) as avg_rating
      FROM movies m
      JOIN ratings r ON m.movieId = r.movieId
      LATERAL VIEW EXPLODE(SPLIT(m.genres, '\\|')) t AS genre
      GROUP BY genre
      ORDER BY movie_count DESC
    """)

    // 输出结果
    genreStats.show(20, truncate = false)

    // 保存到 Parquet
    genreStats.write.mode("overwrite").parquet("output/genre_stats")

    // 导出到 MySQL（如果配置了环境变量）
    val jdbcUrl = sys.env.getOrElse("MYSQL_JDBC_URL", "")
    val mysqlUser = sys.env.getOrElse("MYSQL_USER", "")
    val mysqlPassword = sys.env.getOrElse("MYSQL_PASSWORD", "")

    if (jdbcUrl.nonEmpty && mysqlUser.nonEmpty) {
      val props = MySQLExporter.createProperties(mysqlUser, mysqlPassword)
      MySQLExporter.exportToMySQL(genreStats, "genre_stats", jdbcUrl, props)
    }

    println("类型分析完成")
    spark.stop()
  }
}
