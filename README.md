# SparkMain - Personal Analysis Branch

This branch, `yiyangchen609-web`, contains personal Spark analysis tasks only.
The shared project workflow belongs on the `main` branch.

## Scope

This branch is responsible for personal analysis jobs built on top of the shared
workflow output. It may contain personal realtime analysis tasks, but it does not
own shared Kafka infrastructure, web pages, or deployment scripts for the whole
project.

Shared workflow responsibility:

```text
main branch: Kafka + Spark Streaming + MySQL workflow infrastructure
```

Personal task responsibility:

```text
yiyangchen609-web branch: Spark analysis tasks, personal realtime task, and result tables
```

## Requirements

| Component | Version |
|---|---|
| JDK | 17 |
| Scala | 2.12 |
| Spark | 3.5.0 |
| sbt | 1.9+ |
| Python | 3.8+ |
| MySQL | 8.x, optional for result export |

## Project Layout

```text
SparkMain/
├── SparkMain/
│   ├── build.sbt
│   └── src/main/scala/org/bigdata/
│       ├── Main.scala
│       ├── analysis/
│       │   ├── AnalyzeRatings.scala
│       │   ├── AnalyzeGenres.scala
│       │   ├── AnalyzeTime.scala
│       │   └── AnalyzeUsers.scala
│       └── utils/
│           └── MySQLExporter.scala
├── dataset/
├── dataset_test/
├── cleanPy/
├── cleanPy_test/
├── main_pipeline_new.sh
├── main_pipeline_test.sh
└── test/
    └── verify_results.sh
```

## Analysis Tasks

### `analyze`

Class:

```text
org.bigdata.analysis.AnalyzeRatings
```

Input:

```text
output/ratings_streaming
```

Output:

```text
output/movie_stats
```

Metrics:

- movie rating count
- average rating
- max rating
- min rating

### `genres`

Class:

```text
org.bigdata.analysis.AnalyzeGenres
```

Inputs:

```text
dataset/movies.csv
output/ratings_streaming
```

Output:

```text
output/genre_stats
```

Metrics:

- genre movie count
- genre average rating

### `time`

Class:

```text
org.bigdata.analysis.AnalyzeTime
```

Input:

```text
output/ratings_streaming
```

Outputs:

```text
output/hourly_stats
output/weekday_stats
```

Metrics:

- rating count by hour
- average rating by hour
- rating count by weekday
- average rating by weekday

### `users`

Class:

```text
org.bigdata.analysis.AnalyzeUsers
```

Input:

```text
output/ratings_streaming
```

Outputs:

```text
output/user_activity
output/user_preference
output/rating_distribution
```

Metrics:

- user rating count
- user average rating
- first and last rating time
- user rating preference bucket
- rating distribution and percentage

### `realtime`

Class:

```text
org.bigdata.streaming.PersonalRealtimeRatings
```

Default validation source:

```text
dataset_test/realtime_ratings
```

Outputs:

```text
output/personal_realtime/metrics
output/personal_realtime/top_movies
output/personal_realtime/alerts
```

Metrics:

- 5-minute rating count
- active user count
- active movie count
- average, max, and min rating
- realtime Top10 movies by rating count
- low/high average rating alerts
- realtime Blog overview table (`yc_realtime_overview`) refreshed every batch

The task supports two sources. Kafka mode is the mode used by the Blog realtime
report display.

```bash
# VM/local validation without Kafka
export REALTIME_SOURCE=file
export REALTIME_INPUT_PATH=dataset_test/realtime_ratings

# Kafka mode for Blog realtime display
export REALTIME_SOURCE=kafka
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=ratings_personal_realtime
export KAFKA_STARTING_OFFSETS=latest
export REALTIME_TRIGGER_ONCE=false
export REALTIME_TRIGGER_INTERVAL="2 seconds"
export SPARK_SUBMIT_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
```

## Build

```bash
cd SparkMain
sbt package
```

Expected package:

```text
SparkMain/target/scala-2.12/sparkmain_2.12-1.0.0.jar
```

## Run

Run all personal analysis tasks through the branch script:

```bash
./main_pipeline_new.sh
```

Run one task directly:

```bash
spark-submit \
  --class org.bigdata.Main \
  --master local[*] \
  SparkMain/target/scala-2.12/sparkmain_2.12-1.0.0.jar \
  analyze
```

Available task names:

```text
analyze
genres
time
users
realtime
```

Run the personal realtime validation task:

```bash
./run_personal_realtime.sh
```

Run the Blog realtime demo producer in another terminal. It continuously writes
new Kafka events, advances one 5-minute event-time window about every 4 seconds,
and makes the Blog chart change while the page refreshes:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=ratings_personal_realtime
./run_realtime_demo_producer.sh
```

For a visible Blog demo, keep both processes running:

```bash
# terminal 1: Spark consumes Kafka every 2 seconds
export REALTIME_SOURCE=kafka
export REALTIME_TRIGGER_ONCE=false
export REALTIME_TRIGGER_INTERVAL="2 seconds"
export KAFKA_TOPIC=ratings_personal_realtime
./run_personal_realtime.sh

# terminal 2: producer keeps generating accelerated windows
./run_realtime_demo_producer.sh
```

## MySQL Export

Each analysis task writes Parquet output first. If MySQL environment variables
are provided, the same DataFrame is also exported to MySQL.

```bash
export MYSQL_JDBC_URL="jdbc:mysql://<mysql-host>:3306/sparkmain_results?useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true"
export MYSQL_USER="<mysql-user>"
export MYSQL_PASSWORD="<mysql-password>"
# Use this on the VM if only mysql-connector-java 5.1.x is available.
export MYSQL_DRIVER="com.mysql.jdbc.Driver"
export SPARK_SUBMIT_JARS="/usr/local/hive-2.3.10/lib/mysql-connector-java-5.1.47.jar"
```

For repeatable VM validation, use test table names and clean them before each
run:

```bash
export MYSQL_CLEAN_TABLES=true
export MYSQL_REALTIME_METRICS_TABLE=realtime_rating_metrics_kafka_test
export MYSQL_REALTIME_TOP_MOVIES_TABLE=realtime_top_movies_kafka_test
export MYSQL_REALTIME_ALERTS_TABLE=realtime_rating_alerts_kafka_test
```

Tables written by this branch:

```text
movie_stats
genre_stats
hourly_stats
weekday_stats
user_activity
user_preference
rating_distribution
realtime_rating_metrics
realtime_top_movies
realtime_rating_alerts
yc_realtime_overview
```

## Test Data

Use `dataset_test/` for quick validation. The test data is intentionally small
and suitable for CI or local smoke tests.

```bash
./main_pipeline_test.sh
./test/verify_results.sh
```

## Branch Boundary

Do not add shared workflow infrastructure to this branch. In particular:

- no shared Kafka producer/consumer workflow ownership
- no shared realtime workflow ownership
- no web frontend or web deployment work

Those responsibilities belong to the `main` workflow branch or the web
repositories.
