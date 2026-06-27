package org.bigdata.utils

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode}

object MySQLExporter {

  def createProperties(user: String, password: String): Properties = {
    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    properties
  }

  def exportToMySQL(
      dataFrame: DataFrame,
      table: String,
      jdbcUrl: String,
      properties: Properties,
      saveMode: SaveMode): Unit = {
    dataFrame.write.mode(saveMode).jdbc(jdbcUrl, table, properties)
  }
}
