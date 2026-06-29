#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Fix report #18: Recompute active_day distribution from randomly sampled CSV.

Steps:
  1. Read raw CSV (random sampled from full 3.5GB dataset)
  2. Clean: filter, add event_date/event_hour/weekday columns
  3. Compute active_day distribution per user
  4. Write to Hive table + MySQL table
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, avg, round as spark_round,
    lit, when, hour, to_date, from_unixtime, dayofweek, trim, lower,
    current_timestamp
)
from pyspark.sql.types import LongType, StringType, TimestampType, StructType, StructField

# --- Config ---
RAW_CSV = sys.argv[1] if len(sys.argv) > 1 else "file:///home/master/upload/UserBehavior_random.csv"
HIVE_DB = "bigdata_ana"
VALID_BEHAVIORS = ["pv", "buy", "cart", "fav"]

MYSQL_URL = "jdbc:mysql://127.0.0.1:13306/test_db?useUnicode=true&characterEncoding=utf8&useSSL=false"
MYSQL_USER = "test"
MYSQL_PASSWORD = "test2026"

spark = SparkSession.builder \
    .appName("FixActiveDayDistribution") \
    .config("spark.sql.session.timeZone", "Asia/Shanghai") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

try:
    spark.sql(f"USE {HIVE_DB}")

    # ── Step 1: Read raw CSV ──
    print(f"[INFO] Reading raw CSV: {RAW_CSV}")
    raw_schema = StructType([
        StructField("user_id_raw", StringType(), True),
        StructField("item_id_raw", StringType(), True),
        StructField("category_id_raw", StringType(), True),
        StructField("behavior_type_raw", StringType(), True),
        StructField("timestamp_raw", StringType(), True),
    ])

    raw = spark.read \
        .option("header", "false") \
        .schema(raw_schema) \
        .csv(RAW_CSV)

    # ── Step 2: Clean (same logic as UserBehaviorCleanJob.scala) ──
    print("[INFO] Cleaning data...")
    typed = raw.select(
        col("user_id_raw").cast(LongType()).alias("user_id"),
        col("item_id_raw").cast(LongType()).alias("item_id"),
        col("category_id_raw").cast(LongType()).alias("category_id"),
        lower(trim(col("behavior_type_raw"))).alias("behavior_type"),
        col("timestamp_raw").cast(LongType()).alias("timestamp"),
    )

    event_time = from_unixtime(col("timestamp")).cast(TimestampType())

    cleaned = typed \
        .filter(col("user_id") > 0) \
        .filter(col("item_id") > 0) \
        .filter(col("category_id") > 0) \
        .filter(col("timestamp") > 0) \
        .filter(col("behavior_type").isin(VALID_BEHAVIORS)) \
        .withColumn("event_time", event_time) \
        .withColumn("event_date", to_date(col("event_time")).cast(StringType())) \
        .withColumn("event_hour", hour(col("event_time"))) \
        .withColumn("weekday", ((dayofweek(col("event_time")) + lit(5)) % lit(7)) + lit(1)) \
        .filter(col("event_date").between("2017-11-25", "2017-12-03")) \
        .select("user_id", "item_id", "category_id", "behavior_type",
                "timestamp", "event_time", "event_date", "event_hour", "weekday")

    cleaned.cache()
    total_rows = cleaned.count()
    print(f"[INFO] Cleaned rows: {total_rows:,}")

    # ── Step 3: Compute active_day distribution ──
    print("[INFO] Computing active_day distribution...")
    user_activity = cleaned \
        .filter(col("user_id").isNotNull() & (col("user_id") > 0)) \
        .filter(col("item_id").isNotNull() & (col("item_id") > 0)) \
        .filter(col("event_date").isNotNull() & (col("event_date") != "event_date")) \
        .filter(col("behavior_type").isin(VALID_BEHAVIORS)) \
        .groupBy("user_id") \
        .agg(
            countDistinct(col("event_date")).cast("long").alias("active_days"),
            count(lit(1)).cast("long").alias("behavior_cnt"),
            countDistinct(col("item_id")).cast("long").alias("distinct_item_cnt"),
            count(when(col("behavior_type") == "buy", 1)).cast("long").alias("buy_cnt"),
        )

    total_users_df = user_activity.agg(count(lit(1)).cast("long").alias("total_users"))
    total_users = total_users_df.collect()[0][0]
    print(f"[INFO] Total users: {total_users:,}")

    result = user_activity \
        .groupBy("active_days") \
        .agg(
            count(lit(1)).cast("long").alias("user_cnt"),
            spark_round(avg(col("behavior_cnt")), 2).alias("avg_behavior_cnt"),
            spark_round(avg(col("distinct_item_cnt")), 2).alias("avg_distinct_item_cnt"),
            spark_round(avg(col("buy_cnt")), 2).alias("avg_buy_cnt"),
        ) \
        .crossJoin(total_users_df) \
        .withColumn(
            "user_rate",
            spark_round(col("user_cnt").cast("double") / col("total_users").cast("double"), 4)
        ) \
        .select("active_days", "user_cnt", "user_rate",
                "avg_behavior_cnt", "avg_distinct_item_cnt", "avg_buy_cnt") \
        .orderBy("active_days")

    print("[INFO] New active_day distribution:")
    result.show(20, truncate=False)

    # ── Step 4: Write to Hive ──
    print(f"[INFO] Writing to Hive: {HIVE_DB}.lb_user_active_day_distribution")
    spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.lb_user_active_day_distribution")
    result.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{HIVE_DB}.lb_user_active_day_distribution")

    # ── Step 5: Export to MySQL (using PySpark JDBC overwrite) ──
    print(f"[INFO] Exporting to MySQL...")
    export_df = result.withColumn("spark_exported_at", current_timestamp())

    # For MySQL export with staging pattern, use JDBC directly
    # First write to staging, then promote
    staging_table = "lb_user_active_day_distribution__staging"

    export_df.coalesce(1).write \
        .mode("overwrite") \
        .option("batchsize", "2000") \
        .option("isolationLevel", "NONE") \
        .jdbc(MYSQL_URL, staging_table,
              properties={"user": MYSQL_USER, "password": MYSQL_PASSWORD,
                         "driver": "com.mysql.jdbc.Driver"})

    # Verify staging count
    staging_df = spark.read.jdbc(MYSQL_URL, staging_table,
                                 properties={"user": MYSQL_USER, "password": MYSQL_PASSWORD,
                                            "driver": "com.mysql.jdbc.Driver"})
    staging_count = staging_df.count()
    source_count = result.count()
    print(f"[INFO] Row counts: Hive={source_count}, MySQL staging={staging_count}")

    if staging_count == source_count:
        print("[SUCCESS] All done! Now promoting staging -> target via MySQL...")
        print(f"[ACTION REQUIRED] Run on cluster:")
        print(f"  mysql -h 127.0.0.1 -P 13306 -u test -ptest2026 test_db -e \"")
        print(f"    DROP TABLE IF EXISTS lb_user_active_day_distribution__backup;")
        print(f"    RENAME TABLE lb_user_active_day_distribution TO lb_user_active_day_distribution__backup,")
        print(f"               lb_user_active_day_distribution__staging TO lb_user_active_day_distribution;")
        print(f"    DROP TABLE lb_user_active_day_distribution__backup;\"")
    else:
        print(f"[ERROR] Row count mismatch! Hive={source_count}, MySQL staging={staging_count}")

    print("[SUCCESS] Spark job complete!")
    cleaned.unpersist()

finally:
    spark.stop()
