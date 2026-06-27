name := "SparkMain"
version := "1.0.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
  "mysql" % "mysql-connector-java" % "8.0.33",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)

assembly / mainClass := Some("org.bigdata.Main")
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
