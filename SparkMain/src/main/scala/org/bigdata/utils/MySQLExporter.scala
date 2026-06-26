package org.bigdata.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.util.Properties

/**
 * MySQL 导出工具
 * 将 DataFrame 通过 JDBC 导出到 MySQL
 */
object MySQLExporter {

  /**
   * 导出 DataFrame 到 MySQL
   * @param df 要导出的 DataFrame
   * @param tableName MySQL 表名
   * @param jdbcUrl JDBC 连接 URL
   * @param properties 连接属性（用户名、密码等）
   * @param saveMode 保存模式（Overwrite, Append, Ignore, ErrorIfExists）
   */
  def exportToMySQL(
    df: DataFrame,
    tableName: String,
    jdbcUrl: String,
    properties: Properties,
    saveMode: SaveMode = SaveMode.Overwrite
  ): Unit = {
    df.write
      .mode(saveMode)
      .jdbc(jdbcUrl, tableName, properties)
    println(s"数据已导出到 MySQL 表: $tableName")
  }

  /**
   * 创建 JDBC 连接属性
   * @param username 数据库用户名
   * @param password 数据库密码
   * @return Properties 对象
   */
  def createProperties(username: String, password: String): Properties = {
    val props = new Properties()
    props.put("user", username)
    props.put("password", password)
    props.put("driver", "com.mysql.cj.jdbc.Driver")
    props
  }

  /**
   * 生成 JDBC URL
   * @param host 数据库主机
   * @param port 数据库端口
   * @param database 数据库名
   * @return JDBC URL 字符串
   */
  def generateJdbcUrl(host: String, port: Int, database: String): String = {
    s"jdbc:mysql://$host:$port/$database?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
  }
}
