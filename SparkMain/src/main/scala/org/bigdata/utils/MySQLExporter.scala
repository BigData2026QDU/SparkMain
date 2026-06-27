package org.bigdata.utils

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode}

object MySQLExporter {
  def createProperties(user: String, password: String): Properties = {
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty(
      "driver",
      sys.env.getOrElse("MYSQL_DRIVER", "com.mysql.cj.jdbc.Driver"))
    props
  }

  def exportToMySQL(df: DataFrame, tableName: String, jdbcUrl: String, props: Properties): Unit = {
    df.write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, tableName, props)
  }

  def exportToMySQL(
      df: DataFrame,
      tableName: String,
      jdbcUrl: String,
      props: Properties,
      saveMode: SaveMode): Unit = {
    df.write.mode(saveMode).jdbc(jdbcUrl, tableName, props)
  }
}
