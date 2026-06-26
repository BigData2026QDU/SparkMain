#!/usr/bin/env python3
"""Run the KB-sized Spark/Hive smoke test used by GitHub Actions."""

from __future__ import annotations

import re
import shutil
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType


REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_DB = "bigdata_ana_test"

TABLE_SCHEMAS = {
    "movies": StructType(
        [
            StructField("movieId", IntegerType(), nullable=False),
            StructField("title", StringType(), nullable=False),
            StructField("genres", StringType(), nullable=False),
        ]
    ),
    "ratings": StructType(
        [
            StructField("userId", IntegerType(), nullable=False),
            StructField("movieId", IntegerType(), nullable=False),
            StructField("rating", DoubleType(), nullable=False),
            StructField("timestamp", LongType(), nullable=False),
        ]
    ),
    "tags": StructType(
        [
            StructField("userId", IntegerType(), nullable=False),
            StructField("movieId", IntegerType(), nullable=False),
            StructField("tag", StringType(), nullable=False),
            StructField("timestamp", LongType(), nullable=False),
        ]
    ),
    "links": StructType(
        [
            StructField("movieId", IntegerType(), nullable=False),
            StructField("imdbId", StringType(), nullable=False),
            StructField("tmdbId", IntegerType(), nullable=True),
        ]
    ),
}


def split_sql(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(current).strip().rstrip(";").strip()
            if statement:
                statements.append(statement)
            current = []
    if current:
        statements.append("\n".join(current).strip())
    return statements


def run_sql_dir(spark: SparkSession, directory: Path) -> None:
    for sql_file in sorted(directory.glob("*.sql")):
        print(f"[INFO] Running SQL file: {sql_file.relative_to(REPO_ROOT)}")
        for statement in split_sql(sql_file.read_text(encoding="utf-8")):
            print(f"[SQL] {statement.splitlines()[0][:120]}")
            spark.sql(statement)


def load_csv_tables(spark: SparkSession) -> None:
    data_dir = REPO_ROOT / "dataset_test"
    total_bytes = sum(path.stat().st_size for path in data_dir.glob("*.csv"))
    if total_bytes > 64 * 1024:
        raise AssertionError(f"dataset_test is too large for CI: {total_bytes} bytes")
    print(f"[INFO] dataset_test size: {total_bytes} bytes")

    for table, schema in TABLE_SCHEMAS.items():
        csv_path = data_dir / f"{table}.csv"
        if not csv_path.exists():
            raise FileNotFoundError(csv_path)

        df = spark.read.option("header", True).schema(schema).csv(str(csv_path))
        df.createOrReplaceTempView(f"input_{table}")
        columns = ", ".join(f"`{field.name}`" for field in schema.fields)
        spark.sql(f"INSERT OVERWRITE TABLE {table} SELECT {columns} FROM input_{table}")
        row_count = spark.table(table).count()
        if row_count <= 0:
            raise AssertionError(f"{table} loaded zero rows")
        print(f"[PASS] loaded {table}: {row_count} rows")


def assert_positive_count(spark: SparkSession, table: str) -> None:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table):
        raise ValueError(table)
    count = spark.sql(f"SELECT COUNT(*) AS c FROM {table}").collect()[0]["c"]
    if count <= 0:
        raise AssertionError(f"{table} has no rows")
    print(f"[PASS] {table} has {count} rows")


def run_assertions(spark: SparkSession) -> None:
    expected_tables = [
        "movies",
        "ratings",
        "tags",
        "links",
        "task1_movie_stats",
        "task2_movie_ranking",
        "task3_genre_stats",
    ]
    tables = {row.tableName for row in spark.sql("SHOW TABLES").collect()}
    missing = sorted(set(expected_tables) - tables)
    if missing:
        raise AssertionError(f"missing tables: {', '.join(missing)}")

    for table in expected_tables:
        assert_positive_count(spark, table)

    views = {row.viewName for row in spark.sql("SHOW VIEWS").collect()}
    if "v_movies_ratings" not in views:
        raise AssertionError("missing view: v_movies_ratings")
    print("[PASS] v_movies_ratings view exists")


def main() -> None:
    work_dir = Path(tempfile.mkdtemp(prefix="sparkmain-ci-"))
    try:
        warehouse_dir = work_dir / "warehouse"
        metastore_dir = work_dir / "metastore_db"
        spark = (
            SparkSession.builder.appName("SparkMain CI Spark Hive smoke test")
            .master("local[2]")
            .config("spark.sql.warehouse.dir", warehouse_dir.as_uri())
            .config(
                "spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={metastore_dir};create=true",
            )
            .config("spark.ui.enabled", "false")
            .enableHiveSupport()
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        try:
            spark.sql(f"DROP DATABASE IF EXISTS {TEST_DB} CASCADE")
            run_sql_dir(spark, REPO_ROOT / "initializeSQL_test")
            spark.sql(f"USE {TEST_DB}")
            load_csv_tables(spark)
            run_sql_dir(spark, REPO_ROOT / "prepareData_test")
            run_sql_dir(spark, REPO_ROOT / "jobSQL_test")
            run_assertions(spark)
            print("[SUCCESS] Spark/Hive smoke test completed")
        finally:
            spark.stop()
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
