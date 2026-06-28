ThisBuild / organization := "org.bigdata"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := sys.props
  .get("sparkmain.version")
  .orElse(sys.env.get("SPARKMAIN_VERSION"))
  .orElse(sys.env.get("GITHUB_REF_NAME").collect {
    case ref if ref.startsWith("v") => ref.stripPrefix("v")
  })
  .getOrElse("1.0.0")

name := "SparkMain"

val sparkVersion = "3.5.0"
val kafkaVersion = "3.6.0"
val githubRepository = settingKey[String]("GitHub owner/repository for package publishing")
val stageDistribution = taskKey[File]("Stage a no-sbt SparkMain runtime distribution")

githubRepository := sys.env.getOrElse("GITHUB_REPOSITORY", "BigData2026QDU/SparkMain")

homepage := Some(url(s"https://github.com/${githubRepository.value}"))
scmInfo := Some(
  ScmInfo(
    url(s"https://github.com/${githubRepository.value}"),
    s"scm:git:https://github.com/${githubRepository.value}.git"))
licenses := Seq("Course Project" -> url(s"https://github.com/${githubRepository.value}"))
publishMavenStyle := true
publishTo := Some(
  "GitHub Packages" at s"https://maven.pkg.github.com/${githubRepository.value}")
credentials ++= sys.env.get("GITHUB_TOKEN").toSeq.map { token =>
  Credentials(
    "GitHub Packages",
    "maven.pkg.github.com",
    sys.env.getOrElse("GITHUB_ACTOR", "github-actions"),
    token)
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided,test",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided,test",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided,test",
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
  "mysql" % "mysql-connector-java" % "8.0.33",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)

Test / fork := true
Test / parallelExecution := false
Test / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

stageDistribution := {
  val log = streams.value.log
  val repoRoot = baseDirectory.value.getParentFile
  val distRoot = target.value / "sparkmain-dist" / s"sparkmain-${version.value}"
  val appJar = (Compile / packageBin).value
  val runtimeJars = (Runtime / dependencyClasspath).value.files
    .filter(file => file.isFile && file.getName.endsWith(".jar"))
    .sortBy(_.getName)

  IO.delete(distRoot)
  IO.createDirectory(distRoot / "lib")
  IO.copyFile(appJar, distRoot / "lib" / appJar.getName)
  runtimeJars.foreach { jar =>
    val targetJar = distRoot / "lib" / jar.getName
    if (!targetJar.exists()) {
      IO.copyFile(jar, targetJar)
    }
  }

  def copyFileIfExists(source: File, target: File): Unit = {
    if (source.exists()) {
      IO.createDirectory(target.getParentFile)
      IO.copyFile(source, target)
    }
  }

  def copyDirectoryIfExists(source: File, target: File): Unit = {
    if (source.exists()) {
      IO.copyDirectory(source, target, overwrite = true)
    }
  }

  copyDirectoryIfExists(repoRoot / "release", distRoot)
  copyFileIfExists(repoRoot / "main_pipeline_new.sh", distRoot / "main_pipeline_new.sh")
  copyFileIfExists(repoRoot / "truncate_file.py", distRoot / "truncate_file.py")
  copyFileIfExists(repoRoot / "print_end.sh", distRoot / "print_end.sh")
  copyFileIfExists(
    repoRoot / "dataset" / "download_movielens.py",
    distRoot / "dataset" / "download_movielens.py")
  copyFileIfExists(
    repoRoot / "cleanPy" / "clean_ratings.py",
    distRoot / "cleanPy" / "clean_ratings.py")
  copyDirectoryIfExists(repoRoot / "dataset_test", distRoot / "dataset_test")
  copyDirectoryIfExists(repoRoot / "cleanPy_test", distRoot / "cleanPy_test")
  copyDirectoryIfExists(repoRoot / "test", distRoot / "test")

  log.info(s"Staged SparkMain distribution at ${distRoot.getAbsolutePath}")
  distRoot
}
