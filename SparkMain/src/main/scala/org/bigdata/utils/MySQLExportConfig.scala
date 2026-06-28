package org.bigdata.utils

import java.util.Locale

import org.apache.spark.sql.{DataFrame, SaveMode}

sealed trait MySQLExportConfig {
  def enabled: Boolean
}

object MySQLExportConfig {
  final case class Enabled(
      jdbcUrl: String,
      user: String,
      password: String,
      table: String)
      extends MySQLExportConfig {
    override val enabled: Boolean = true
  }

  case object Disabled extends MySQLExportConfig {
    override val enabled: Boolean = false
  }

  def fromEnvironment(
      tableEnvVars: Seq[String],
      env: Map[String, String] = sys.env,
      enabledEnvVar: String = "MYSQL_ENABLED"): MySQLExportConfig = {
    if (!flag(env, enabledEnvVar, default = false)) {
      Disabled
    } else {
      val tableCandidates = tableEnvVars.distinct
      val table = firstNonEmpty(env, tableCandidates).getOrElse {
        throw new IllegalArgumentException(
          s"${tableCandidates.mkString(" or ")} is required when $enabledEnvVar=true")
      }

      Enabled(
        jdbcUrl = requiredNonEmpty(env, "MYSQL_JDBC_URL", enabledEnvVar),
        user = requiredNonEmpty(env, "MYSQL_USER", enabledEnvVar),
        password = requiredPresent(env, "MYSQL_PASSWORD", enabledEnvVar),
        table = table)
    }
  }

  def exportIfEnabled(
      dataFrame: DataFrame,
      config: MySQLExportConfig,
      saveMode: SaveMode): Boolean = {
    config match {
      case Enabled(jdbcUrl, user, password, table) =>
        val props = MySQLExporter.createProperties(user, password)
        MySQLExporter.exportToMySQL(dataFrame, table, jdbcUrl, props, saveMode)
        true
      case Disabled =>
        false
    }
  }

  def tableName(config: MySQLExportConfig): Option[String] = config match {
    case Enabled(_, _, _, table) => Some(table)
    case Disabled => None
  }

  private def flag(
      env: Map[String, String],
      name: String,
      default: Boolean): Boolean = {
    env.get(name).map(_.trim.toLowerCase(Locale.ROOT)) match {
      case Some("true") | Some("1") | Some("yes") => true
      case Some("false") | Some("0") | Some("no") => false
      case Some(other) =>
        throw new IllegalArgumentException(
          s"$name must be true or false, but was '$other'")
      case None => default
    }
  }

  private def firstNonEmpty(
      env: Map[String, String],
      names: Seq[String]): Option[String] = {
    names.iterator
      .flatMap(name => env.get(name).map(_.trim).filter(_.nonEmpty))
      .toSeq
      .headOption
  }

  private def requiredNonEmpty(
      env: Map[String, String],
      name: String,
      enabledEnvVar: String): String = {
    env.get(name).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new IllegalArgumentException(
        s"$name is required when $enabledEnvVar=true")
    }
  }

  private def requiredPresent(
      env: Map[String, String],
      name: String,
      enabledEnvVar: String): String = {
    env.getOrElse(
      name,
      throw new IllegalArgumentException(
        s"$name is required when $enabledEnvVar=true"))
  }
}
