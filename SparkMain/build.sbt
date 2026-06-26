name := "SparkMain"
version := "1.0.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"
val kafkaVersion = "3.6.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
  "mysql" % "mysql-connector-java" % "8.0.33",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)
