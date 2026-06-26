#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Truncate CSV files by byte size while preserving whole lines."""

import argparse
import os
import shutil
import sys

DEFAULT_TARGET_SIZE_MB = 300


def truncate_file(input_file, output_file, target_size_mb=DEFAULT_TARGET_SIZE_MB):
    target_size = target_size_mb * 1024 * 1024

    if not os.path.exists(input_file):
        print(f"[ERROR] Input file does not exist: {input_file}")
        sys.exit(1)

    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    file_size = os.path.getsize(input_file)
    print(f"[INFO] Input file: {input_file}")
    print(f"[INFO] Input size: {file_size / 1024 / 1024:.2f} MB")
    print(f"[INFO] Target size: {target_size_mb} MB")

    if file_size <= target_size:
        shutil.copy2(input_file, output_file)
        print(f"[INFO] Copied without truncation: {output_file}")
        return

    total_bytes = 0
    lines_written = 0

    try:
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
        truncate_file(input_path, output_path, target_size_mb)

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
        truncate_file(args.input_file, output, args.size)
    else:
        process_dataset_directory(args.dataset_dir, args.output_dir, args.size)


if __name__ == "__main__":
    main()
