package org.example.analysis

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.example.utils.MySQLExporter

/**
 * 用户行为分析任务
 * 分析用户评分行为和活跃度
 */
object AnalyzeUsers {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AnalyzeUsers")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    // 读取评分数据
    val ratingsPath = if (args.length > 0) args(0) else "output/ratings_streaming"
    val ratings = spark.read.parquet(ratingsPath)

    // 创建临时视图
    ratings.createOrReplaceTempView("ratings")

    // 分析1: 用户评分活跃度
    val userActivity = spark.sql("""
      SELECT 
        userId,
        COUNT(*) as rating_count,
        ROUND(AVG(rating), 2) as avg_rating,
        MIN(FROM_UNIXTIME(timestamp)) as first_rating,
        MAX(FROM_UNIXTIME(timestamp)) as last_rating
      FROM ratings
      GROUP BY userId
      ORDER BY rating_count DESC
    """)

    println("=== 用户活跃度 ===")
    userActivity.show(20, truncate = false)

    // 分析2: 用户评分偏好
    val userPreference = spark.sql("""
      SELECT 
        userId,
        CASE 
          WHEN AVG(rating) >= 4.0 THEN '高分偏好'
          WHEN AVG(rating) >= 3.0 THEN '中分偏好'
          ELSE '低分偏好'
        END as rating_preference,
        COUNT(*) as count
      FROM ratings
      GROUP BY userId
      ORDER BY userId
    """)

    println("=== 用户评分偏好 ===")
    userPreference.show(20, truncate = false)

    // 分析3: 评分分布
    val ratingDistribution = spark.sql("""
      SELECT 
        rating,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ratings), 2) as percentage
      FROM ratings
      GROUP BY rating
      ORDER BY rating
    """)

    println("=== 评分分布 ===")
    ratingDistribution.show(10, truncate = false)

    // 保存结果
    userActivity.write.mode("overwrite").parquet("output/user_activity")
    userPreference.write.mode("overwrite").parquet("output/user_preference")
    ratingDistribution.write.mode("overwrite").parquet("output/rating_distribution")

    // 导出到 MySQL（如果配置了环境变量）
    val jdbcUrl = sys.env.getOrElse("MYSQL_JDBC_URL", "")
    val mysqlUser = sys.env.getOrElse("MYSQL_USER", "")
    val mysqlPassword = sys.env.getOrElse("MYSQL_PASSWORD", "")

    if (jdbcUrl.nonEmpty && mysqlUser.nonEmpty) {
      val props = MySQLExporter.createProperties(mysqlUser, mysqlPassword)
      MySQLExporter.exportToMySQL(userActivity, "user_activity", jdbcUrl, props)
      MySQLExporter.exportToMySQL(userPreference, "user_preference", jdbcUrl, props)
      MySQLExporter.exportToMySQL(ratingDistribution, "rating_distribution", jdbcUrl, props)
    }

    println("用户行为分析完成")
    spark.stop()
  }
}
