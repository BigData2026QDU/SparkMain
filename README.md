# SparkMain - Personal Analysis Branch

This branch, `yiyangchen609-web`, contains personal Spark analysis tasks only.
The shared project workflow belongs on the `main` branch.

## Scope

This branch is responsible for offline analysis jobs built on top of the shared
workflow output. It does not own Kafka, streaming infrastructure, web pages, or
deployment scripts for the whole project.

Shared workflow responsibility:

```text
main branch: Kafka + Spark Streaming + MySQL workflow infrastructure
```

Personal task responsibility:

```text
yiyangchen609-web branch: Spark analysis tasks and result tables
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
```

## MySQL Export

Each analysis task writes Parquet output first. If MySQL environment variables
are provided, the same DataFrame is also exported to MySQL.

```bash
export MYSQL_JDBC_URL="jdbc:mysql://192.168.56.1:3306/sparkmain_results?useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true"
export MYSQL_USER="root"
export MYSQL_PASSWORD="root"
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

- no Kafka producer/consumer workflow ownership
- no shared realtime workflow ownership
- no web frontend or web deployment work

Those responsibilities belong to the `main` workflow branch or the web
repositories.
