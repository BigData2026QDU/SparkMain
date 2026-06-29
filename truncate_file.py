#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Truncate CSV files by byte size while preserving whole lines.

支持两种模式：
  - random（默认）：随机采样，保留原始分布特征，适合用于分析报告
  - head：取文件前 N 字节（线性截断），可能导致样本偏差
"""

import argparse
import os
import random
import shutil
import sys

DEFAULT_TARGET_SIZE_MB = 200


def truncate_file_head(input_file, output_file, target_size):
    """线性截断：取文件前 target_size 字节（保留整行）。"""
    total_bytes = 0
    lines_written = 0

    with open(input_file, "r", encoding="utf-8", newline="") as infile, open(
        output_file, "w", encoding="utf-8", newline=""
    ) as outfile:
        for line in infile:
            line_size = len(line.encode("utf-8"))
            if total_bytes > 0 and total_bytes + line_size > target_size:
                break
            outfile.write(line)
            total_bytes += line_size
            lines_written += 1
            if lines_written % 100000 == 0:
                print(
                    f"[INFO] Written {lines_written:,} lines, "
                    f"{total_bytes / 1024 / 1024:.2f} MB"
                )

    return lines_written, total_bytes


def truncate_file_random(input_file, output_file, target_size):
    """随机采样：以等概率随机选取行，使输出接近 target_size 且保留原始分布。

    采用单遍随机采样算法：对每行以 p = target_size / file_size 的概率保留，
    期望输出大小接近 target_size。为避免随机波动导致过大偏离，当已写入字节
    超过 target_size 的 105% 时提前终止。
    """
    file_size = os.path.getsize(input_file)
    # 概率略保守，配合上限检查确保不超出太多
    p = min(target_size / file_size, 0.99)

    random.seed(42)  # 固定种子，保证可复现

    total_bytes = 0
    lines_written = 0
    lines_read = 0
    cap = int(target_size * 1.05)  # 105% 硬上限

    with open(input_file, "r", encoding="utf-8", newline="") as infile, open(
        output_file, "w", encoding="utf-8", newline=""
    ) as outfile:
        for line in infile:
            lines_read += 1
            line_size = len(line.encode("utf-8"))

            if total_bytes + line_size > cap:
                break

            if random.random() < p:
                outfile.write(line)
                total_bytes += line_size
                lines_written += 1

            if lines_read % 500000 == 0:
                print(
                    f"[INFO] Read {lines_read:,} lines, "
                    f"written {lines_written:,} lines, "
                    f"{total_bytes / 1024 / 1024:.2f} MB"
                )

    return lines_written, total_bytes


def truncate_file(
    input_file, output_file, target_size_mb=DEFAULT_TARGET_SIZE_MB, mode="random"
):
    target_size = target_size_mb * 1024 * 1024

    if not os.path.exists(input_file):
        print(f"[ERROR] Input file does not exist: {input_file}")
        sys.exit(1)

    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    file_size = os.path.getsize(input_file)
    print(f"[INFO] Input file: {input_file}")
    print(f"[INFO] Input size: {file_size / 1024 / 1024:.2f} MB")
    print(f"[INFO] Target size: {target_size_mb} MB")
    print(f"[INFO] Mode: {mode}")

    if file_size <= target_size:
        shutil.copy2(input_file, output_file)
        print(f"[INFO] Copied without truncation: {output_file}")
        return

    try:
        if mode == "head":
            lines_written, total_bytes = truncate_file_head(
                input_file, output_file, target_size
            )
        else:
            lines_written, total_bytes = truncate_file_random(
                input_file, output_file, target_size
            )

        print(f"[SUCCESS] Output file: {output_file}")
        print(f"  lines_written: {lines_written:,}")
        print(f"  output_size_mb: {total_bytes / 1024 / 1024:.2f}")
    except OSError as exc:
        print(f"[ERROR] File operation failed: {exc}")
        sys.exit(1)


def process_dataset_directory(
    dataset_dir="dataset",
    output_dir="truncatedDataset",
    target_size_mb=DEFAULT_TARGET_SIZE_MB,
    mode="random",
):
    if not os.path.exists(dataset_dir):
        print(f"[ERROR] Dataset directory does not exist: {dataset_dir}")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)
    csv_files = sorted(name for name in os.listdir(dataset_dir) if name.endswith(".csv"))
    if not csv_files:
        print(f"[WARNING] No CSV files found in {dataset_dir}")
        return

    print(f"[INFO] Found {len(csv_files)} CSV files in {dataset_dir}")
    for csv_file in csv_files:
        input_path = os.path.join(dataset_dir, csv_file)
        output_path = os.path.join(output_dir, csv_file)
        print(f"[INFO] Processing {csv_file}")
        truncate_file(input_path, output_path, target_size_mb, mode)

    print(f"[SUCCESS] Truncated files written to {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="Truncate CSV files to a target size for Spark/Hive analysis."
    )
    parser.add_argument("input_file", nargs="?", help="Optional input CSV file")
    parser.add_argument("-o", "--output", help="Output CSV path")
    parser.add_argument(
        "--size",
        type=int,
        default=DEFAULT_TARGET_SIZE_MB,
        help=f"Target size in MB, default {DEFAULT_TARGET_SIZE_MB}",
    )
    parser.add_argument(
        "--mode",
        choices=["random", "head"],
        default="random",
        help="截断模式：random=随机采样（默认，保留原始分布）；head=取文件头部（线性截断，可能偏差）",
    )
    parser.add_argument(
        "--dataset-dir",
        default="dataset",
        help="Dataset directory for batch mode, default dataset",
    )
    parser.add_argument(
        "--output-dir",
        default="truncatedDataset",
        help="Output directory for batch mode, default truncatedDataset",
    )
    parser.add_argument("--status", action="store_true", help="Print CSV file sizes")

    args = parser.parse_args()

    if args.status:
        dataset_dir = args.dataset_dir
        if args.input_file:
            paths = [args.input_file]
        else:
            paths = [
                os.path.join(dataset_dir, name)
                for name in sorted(os.listdir(dataset_dir))
                if name.endswith(".csv")
            ] if os.path.exists(dataset_dir) else []

        for path in paths:
            if os.path.exists(path):
                print(f"{path}: {os.path.getsize(path) / 1024 / 1024:.2f} MB")
            else:
                print(f"{path}: missing")
        return

    if args.input_file:
        output = args.output
        if output is None:
            output = os.path.join(args.output_dir, os.path.basename(args.input_file))
        truncate_file(args.input_file, output, args.size, args.mode)
    else:
        process_dataset_directory(
            args.dataset_dir, args.output_dir, args.size, args.mode
        )


if __name__ == "__main__":
    main()
