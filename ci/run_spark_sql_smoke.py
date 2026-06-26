#!/usr/bin/env python3
"""Run the KB-sized Spark SQL smoke test used by GitHub Actions."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


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


def main() -> None:
    dataset_dir = REPO_ROOT / "dataset_test"
    files = sorted(dataset_dir.glob("*.csv"))
    total_bytes = sum(path.stat().st_size for path in files)
    print(f"dataset_test files: {[path.name for path in files]}")
    print(f"dataset_test total bytes: {total_bytes}")
    if not files:
        raise SystemExit("dataset_test has no CSV files")
    if total_bytes > 64 * 1024:
        raise SystemExit("dataset_test must stay below 64 KiB for CI")

    warehouse_dir = REPO_ROOT / "spark-warehouse-ci"
    output_dir = REPO_ROOT / "output" / "ci"
    shutil.rmtree(warehouse_dir, ignore_errors=True)
    shutil.rmtree(output_dir, ignore_errors=True)

    jar_path = REPO_ROOT / "SparkMain" / "target" / "spark-streaming-kafka-1.0.0.jar"
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
        str(REPO_ROOT / "cleanedDataset_test"),
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


if __name__ == "__main__":
    main()
