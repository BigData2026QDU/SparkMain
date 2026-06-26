#!/usr/bin/env python3
"""Run the KB-sized Spark SQL smoke test used by GitHub Actions."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_EXPORTS = (
    "task1_movie_stats",
    "task2_movie_ranking",
    "task3_genre_stats",
)


def spark_environment() -> dict[str, str]:
    env = os.environ.copy()
    if "SPARK_HOME" not in env:
        try:
            import pyspark  # type: ignore

            env["SPARK_HOME"] = str(Path(pyspark.__file__).resolve().parent)
        except ImportError:
            pass
    env.setdefault("PYSPARK_PYTHON", os.environ.get("PYTHON", "python"))
    return env


def verify_lightweight_dataset(dataset_dir: Path) -> None:
    files = sorted(dataset_dir.glob("*.csv"))
    total_bytes = sum(path.stat().st_size for path in files)
    print(f"dataset_test files: {[path.name for path in files]}", flush=True)
    print(f"dataset_test total bytes: {total_bytes}", flush=True)
    if not files:
        raise SystemExit("dataset_test has no CSV files")
    if total_bytes > 64 * 1024:
        raise SystemExit("dataset_test must stay below 64 KiB for CI")


def run_test_cleaners(cleaner_dir: Path, cleaned_dir: Path) -> None:
    cleaners = sorted(cleaner_dir.glob("*.py"))
    if not cleaners:
        raise SystemExit(f"{cleaner_dir.relative_to(REPO_ROOT)} has no cleaner scripts")

    shutil.rmtree(cleaned_dir, ignore_errors=True)
    cleaned_dir.mkdir(parents=True, exist_ok=True)
    for cleaner in cleaners:
        print(f"[INFO] Running cleaner: {cleaner.relative_to(REPO_ROOT)}", flush=True)
        subprocess.run([sys.executable, str(cleaner)], cwd=REPO_ROOT, check=True)


def verify_exported_results(output_dir: Path) -> None:
    for table_name in EXPECTED_EXPORTS:
        table_dir = output_dir / table_name
        csv_files = sorted(table_dir.glob("*.csv"))
        if not csv_files:
            raise SystemExit(f"Missing exported CSV for {table_name} in {table_dir}")

        data_lines = 0
        for csv_file in csv_files:
            with csv_file.open("r", encoding="utf-8") as handle:
                # Skip the header line written by Spark.
                data_lines += max(sum(1 for _ in handle) - 1, 0)

        if data_lines == 0:
            raise SystemExit(f"Exported result {table_name} has no data rows")
        print(f"[PASS] Exported {table_name} with {data_lines} rows", flush=True)


def main() -> None:
    dataset_dir = REPO_ROOT / "dataset_test"
    cleaned_dir = REPO_ROOT / "cleanedDataset_test"
    cleaner_dir = REPO_ROOT / "cleanPy_test"
    warehouse_dir = REPO_ROOT / "spark-warehouse-ci"
    output_dir = REPO_ROOT / "output" / "ci"
    jar_path = REPO_ROOT / "SparkMain" / "target" / "spark-streaming-kafka-1.0.0.jar"

    verify_lightweight_dataset(dataset_dir)
    run_test_cleaners(cleaner_dir, cleaned_dir)
    if not jar_path.is_file():
        raise SystemExit(f"Missing built jar: {jar_path}. Run Maven package before this smoke test.")

    shutil.rmtree(warehouse_dir, ignore_errors=True)
    shutil.rmtree(output_dir, ignore_errors=True)

    env = spark_environment()
    spark_submit = shutil.which(os.environ.get("SPARK_SUBMIT_CMD", "spark-submit"), path=env.get("PATH"))
    if spark_submit is None:
        raise SystemExit("spark-submit was not found on PATH")

    cmd = [
        spark_submit,
        "--class",
        "org.example.pipeline.SparkSqlPipeline",
        "--master",
        "local[2]",
        str(jar_path),
        "--database",
        "bigdata_ana_test",
        "--dataset-dir",
        str(REPO_ROOT / "dataset_test"),
        "--cleaned-dir",
        str(cleaned_dir),
        "--prepare-dir",
        str(REPO_ROOT / "prepareData_test"),
        "--job-dir",
        str(REPO_ROOT / "jobSQL_test"),
        "--warehouse-dir",
        str(warehouse_dir),
        "--output-dir",
        str(output_dir),
        "--validate",
    ]
    subprocess.run(cmd, cwd=REPO_ROOT, check=True, env=env)
    verify_exported_results(output_dir)


if __name__ == "__main__":
    main()
