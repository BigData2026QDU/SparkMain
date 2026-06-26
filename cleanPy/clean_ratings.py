#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
评分数据清洗脚本
清洗 MovieLens ratings.csv 数据
"""

import os
import csv
import sys

INPUT_DIR = "dataset"
OUTPUT_DIR = "cleanedDataset"

def clean_ratings():
    """清洗评分数据"""
    print("=" * 60)
    print("评分数据清洗工具")
    print("=" * 60)
    print()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    input_file = os.path.join(INPUT_DIR, "ratings.csv")
    output_file = os.path.join(OUTPUT_DIR, "ratings.csv")

    if not os.path.exists(input_file):
        print(f"[跳过] 输入文件不存在: {input_file}")
        return

    print(f"[信息] 输入文件: {input_file}")
    print(f"[信息] 输出文件: {output_file}")
    print()

    total_rows = 0
    valid_rows = 0
    skipped_rows = 0

    try:
        with open(input_file, 'r', encoding='utf-8') as infile:
            with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
                reader = csv.reader(infile)
                writer = csv.writer(outfile)

                header = next(reader)
                writer.writerow(header)

                for row in reader:
                    total_rows += 1

                    if len(row) != 4:
                        skipped_rows += 1
                        continue

                    try:
                        userId = int(row[0])
                        movieId = int(row[1])
                        rating = float(row[2])
                        timestamp = int(row[3])

                        if rating < 0.5 or rating > 5.0:
                            skipped_rows += 1
                            continue

                        writer.writerow([userId, movieId, rating, timestamp])
                        valid_rows += 1

                    except (ValueError, IndexError):
                        skipped_rows += 1
                        continue

                    if total_rows % 1000000 == 0:
                        print(f"  已处理: {total_rows} 行")

        print()
        print(f"[成功] 数据清洗完成")
        print(f"  总行数: {total_rows}")
        print(f"  有效行: {valid_rows}")
        print(f"  跳过行: {skipped_rows}")
        print(f"  输出文件: {output_file}")

    except Exception as e:
        print(f"[错误] 清洗失败: {e}")
        sys.exit(1)

if __name__ == '__main__':
    clean_ratings()
