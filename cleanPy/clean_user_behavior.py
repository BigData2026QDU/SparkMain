#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Clean Taobao UserBehavior.csv into a Hive-friendly CSV."""

import csv
import os
import sys
from datetime import datetime

INPUT_DIR = "truncatedDataset"
FALLBACK_INPUT_DIR = "dataset"
OUTPUT_DIR = "cleanedDataset"
INPUT_FILE_NAME = "UserBehavior.csv"
OUTPUT_FILE_NAME = "user_behavior.csv"
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


def resolve_input_file():
    for input_dir in (INPUT_DIR, FALLBACK_INPUT_DIR):
        path = os.path.join(input_dir, INPUT_FILE_NAME)
        if os.path.exists(path):
            return path
    return None


def clean_user_behavior():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    input_file = resolve_input_file()
    output_file = os.path.join(OUTPUT_DIR, OUTPUT_FILE_NAME)

    if input_file is None:
        print(f"[跳过] 未找到 {INPUT_FILE_NAME}")
        return

    total_rows = 0
    valid_rows = 0
    skipped_rows = 0

    with open(input_file, "r", encoding="utf-8", newline="") as infile, open(
        output_file, "w", encoding="utf-8", newline=""
    ) as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        writer.writerow(HEADER)

        for row in reader:
            total_rows += 1
            if len(row) != 5:
                skipped_rows += 1
                continue

            try:
                user_id = int(row[0])
                item_id = int(row[1])
                category_id = int(row[2])
                behavior_type = row[3].strip()
                event_ts = int(row[4])
            except ValueError:
                skipped_rows += 1
                continue

            if behavior_type not in VALID_BEHAVIORS or event_ts <= 0:
                skipped_rows += 1
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
            valid_rows += 1

            if total_rows % 1000000 == 0:
                print(f"  已处理: {total_rows} 行")

    print("[成功] 用户行为数据清洗完成")
    print(f"  输入文件: {input_file}")
    print(f"  输出文件: {output_file}")
    print(f"  总行数: {total_rows}")
    print(f"  有效行: {valid_rows}")
    print(f"  跳过行: {skipped_rows}")

    if valid_rows == 0:
        print("[错误] 没有清洗出有效用户行为数据")
        sys.exit(1)


if __name__ == "__main__":
    clean_user_behavior()
