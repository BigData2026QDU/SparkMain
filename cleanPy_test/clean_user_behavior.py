#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Clean lightweight UserBehavior test data."""

import csv
import os
from datetime import datetime

INPUT_FILE = os.path.join("dataset_test", "UserBehavior.csv")
OUTPUT_DIR = "cleanedDataset_test"
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


def clean_user_behavior():
    if not os.path.exists(INPUT_FILE):
        print(f"跳过用户行为数据清洗: {INPUT_FILE} 不存在")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    total = 0
    valid = 0
    skipped = 0

    with open(INPUT_FILE, "r", encoding="utf-8", newline="") as infile, open(
        OUTPUT_FILE, "w", encoding="utf-8", newline=""
    ) as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        writer.writerow(HEADER)

        for row in reader:
            total += 1
            try:
                if len(row) != 5:
                    skipped += 1
                    continue
                user_id = int(row[0])
                item_id = int(row[1])
                category_id = int(row[2])
                behavior_type = row[3].strip()
                event_ts = int(row[4])
                if behavior_type not in VALID_BEHAVIORS or event_ts <= 0:
                    skipped += 1
                    continue
            except ValueError:
                skipped += 1
                continue

            event_dt = datetime.fromtimestamp(event_ts)
            writer.writerow(
                [
                    user_id,
                    item_id,
                    category_id,
                    behavior_type,
                    event_ts,
                    event_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    event_dt.strftime("%Y-%m-%d"),
                    event_dt.hour,
                    event_dt.isoweekday(),
                ]
            )
            valid += 1

    print("用户行为数据清洗完成:")
    print(f"  总行数: {total}")
    print(f"  有效行数: {valid}")
    print(f"  跳过行数: {skipped}")


if __name__ == "__main__":
    clean_user_behavior()
