#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Clean Taobao UserBehavior.csv into a Hive-friendly CSV for issue #14."""

import csv
import os
import sys
from datetime import datetime, timedelta, timezone

INPUT_CANDIDATES = (
    os.path.join("truncatedDataset", "UserBehavior.csv"),
    os.path.join("dataset", "UserBehavior.csv"),
)
OUTPUT_DIR = "cleanedDataset"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "user_behavior.csv")
VALID_BEHAVIORS = {"pv", "buy", "cart", "fav"}
HEADER = [
    "user_id",
    "item_id",
    "category_id",
    "behavior_type",
    "timestamp",
    "event_time",
    "event_date",
    "event_hour",
    "weekday",
]

CHINA_TZ = timezone(timedelta(hours=8))
MIN_EVENT_DATE = datetime(2017, 11, 25).date()
MAX_EVENT_DATE = datetime(2017, 12, 3).date()
MIN_VALID_ROWS = int(os.getenv("USER_BEHAVIOR_MIN_ROWS", "15000"))
MAX_OUTPUT_SIZE_MB = int(os.getenv("USER_BEHAVIOR_MAX_OUTPUT_MB", "500"))


def resolve_input_file():
    for path in INPUT_CANDIDATES:
        if os.path.exists(path):
            return path
    return None


def parse_row(row):
    if len(row) != 5:
        return None, "bad_column_count"

    try:
        user_id = int(row[0])
        item_id = int(row[1])
        category_id = int(row[2])
        behavior_type = row[3].strip().lower()
        event_ts = int(row[4])
    except ValueError:
        return None, "bad_type"

    if user_id <= 0 or item_id <= 0 or category_id <= 0:
        return None, "non_positive_id"
    if behavior_type not in VALID_BEHAVIORS:
        return None, "bad_behavior"
    if event_ts <= 0:
        return None, "bad_timestamp"

    event_dt = datetime.fromtimestamp(event_ts, CHINA_TZ).replace(tzinfo=None)
    if not (MIN_EVENT_DATE <= event_dt.date() <= MAX_EVENT_DATE):
        return None, "out_of_range_time"

    return (
        user_id,
        item_id,
        category_id,
        behavior_type,
        event_ts,
        event_dt.strftime("%Y-%m-%d %H:%M:%S"),
        event_dt.strftime("%Y-%m-%d"),
        event_dt.hour,
        event_dt.isoweekday(),
    ), None


def clean_user_behavior():
    input_file = resolve_input_file()
    if input_file is None:
        print("[SKIP] UserBehavior.csv not found in truncatedDataset/ or dataset/")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    total_rows = 0
    valid_rows = 0
    skipped_rows = 0
    skip_reasons = {
        "bad_column_count": 0,
        "bad_type": 0,
        "non_positive_id": 0,
        "bad_behavior": 0,
        "bad_timestamp": 0,
        "out_of_range_time": 0,
    }

    with open(input_file, "r", encoding="utf-8", newline="") as infile, open(
        OUTPUT_FILE, "w", encoding="utf-8", newline=""
    ) as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        writer.writerow(HEADER)

        for row in reader:
            total_rows += 1
            parsed, reason = parse_row(row)
            if parsed is None:
                skipped_rows += 1
                skip_reasons[reason] += 1
                continue

            writer.writerow(parsed)
            valid_rows += 1

            if total_rows % 1000000 == 0:
                print(f"[INFO] Processed {total_rows:,} rows, valid {valid_rows:,}")

    output_size_mb = os.path.getsize(OUTPUT_FILE) / 1024 / 1024
    print("[SUCCESS] UserBehavior cleaning completed")
    print(f"  input_file: {input_file}")
    print(f"  output_file: {OUTPUT_FILE}")
    print(f"  total_rows: {total_rows:,}")
    print(f"  valid_rows: {valid_rows:,}")
    print(f"  skipped_rows: {skipped_rows:,}")
    print(f"  output_size_mb: {output_size_mb:.2f}")
    for reason, count in skip_reasons.items():
        if count:
            print(f"  skipped_{reason}: {count:,}")

    if valid_rows == 0:
        print("[ERROR] No valid UserBehavior rows were written")
        sys.exit(1)
    if valid_rows < MIN_VALID_ROWS:
        print(
            f"[ERROR] UserBehavior output has {valid_rows:,} rows; "
            f"issue #14 requires at least {MIN_VALID_ROWS:,} production rows"
        )
        sys.exit(1)
    if output_size_mb > MAX_OUTPUT_SIZE_MB:
        print(
            f"[ERROR] UserBehavior output is {output_size_mb:.2f} MB; "
            f"limit is {MAX_OUTPUT_SIZE_MB} MB"
        )
        sys.exit(1)


if __name__ == "__main__":
    clean_user_behavior()
