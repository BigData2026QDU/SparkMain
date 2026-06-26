package org.bigdata.analysis

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.bigdata.utils.MySQLExporter

/**
 * 时间分析任务
 * 分析评分的时间分布和趋势
 */
object AnalyzeTime {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AnalyzeTime")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    // 读取评分数据
    val ratingsPath = if (args.length > 0) args(0) else "output/ratings_streaming"
    val ratings = spark.read.parquet(ratingsPath)

    // 创建临时视图
    ratings.createOrReplaceTempView("ratings")

    // 分析1: 按小时统计评分数量
    val hourlyStats = spark.sql("""
      SELECT 
        HOUR(FROM_UNIXTIME(timestamp)) as hour,
        COUNT(*) as rating_count,
        ROUND(AVG(rating), 2) as avg_rating
      FROM ratings
      GROUP BY HOUR(FROM_UNIXTIME(timestamp))
      ORDER BY hour
    """)

    println("=== 按小时统计 ===")
    hourlyStats.show(24, truncate = false)

    // 分析2: 按星期统计评分数量
    val weekdayStats = spark.sql("""
      SELECT 
        DAYOFWEEK(FROM_UNIXTIME(timestamp)) as day_of_week,
        COUNT(*) as rating_count,
        ROUND(AVG(rating), 2) as avg_rating
      FROM ratings
      GROUP BY DAYOFWEEK(FROM_UNIXTIME(timestamp))
      ORDER BY day_of_week
    """)

    println("=== 按星期统计 ===")
    weekdayStats.show(7, truncate = false)

    // 保存结果
    hourlyStats.write.mode("overwrite").parquet("output/hourly_stats")
    weekdayStats.write.mode("overwrite").parquet("output/weekday_stats")

    // 导出到 MySQL（如果配置了环境变量）
    val jdbcUrl = sys.env.getOrElse("MYSQL_JDBC_URL", "")
    val mysqlUser = sys.env.getOrElse("MYSQL_USER", "")
    val mysqlPassword = sys.env.getOrElse("MYSQL_PASSWORD", "")

    if (jdbcUrl.nonEmpty && mysqlUser.nonEmpty) {
      val props = MySQLExporter.createProperties(mysqlUser, mysqlPassword)
      MySQLExporter.exportToMySQL(hourlyStats, "hourly_stats", jdbcUrl, props)
      MySQLExporter.exportToMySQL(weekdayStats, "weekday_stats", jdbcUrl, props)
    }

    println("时间分析完成")
    spark.stop()
  }
}
