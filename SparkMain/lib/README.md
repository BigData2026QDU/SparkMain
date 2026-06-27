# Offline runtime dependencies

These two JARs are vendored because the CentOS acceptance VM cannot access
Maven Central:

- `spark-sql-kafka-0-10_2.11-2.4.6.jar`
- `kafka-clients-2.0.0.jar`

They match Spark 2.4.6 and Scala 2.11.12 on `hadoop101`. The sbt build still
declares the normal Maven dependencies for Spark 3.5 / Scala 2.12 development.
