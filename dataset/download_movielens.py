#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MovieLens 数据集下载脚本
下载 MovieLens 25M 数据集到 dataset 目录
"""

import os
import zipfile
import urllib.request
import sys

MOVIELENS_URL = "https://files.grouplens.org/datasets/movielens/ml-25m.zip"
TEMP_DIR = "temp_download"
TARGET_DIR = "dataset"

def download_movielens():
    """下载 MovieLens 25M 数据集"""
    print("=" * 60)
    print("MovieLens 25M 数据集下载工具")
    print("=" * 60)
    print()

    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(TARGET_DIR, exist_ok=True)

    zip_path = os.path.join(TEMP_DIR, "ml-25m.zip")

    if os.path.exists(zip_path):
        print(f"[信息] 文件已存在: {zip_path}")
    else:
        print(f"[信息] 开始下载 MovieLens 25M 数据集...")
        print(f"[信息] URL: {MOVIELENS_URL}")
        print(f"[信息] 目标: {zip_path}")
        print()

        try:
            urllib.request.urlretrieve(MOVIELENS_URL, zip_path)
            print(f"[成功] 下载完成")
        except Exception as e:
            print(f"[错误] 下载失败: {e}")
            sys.exit(1)

    print()
    print("[信息] 正在解压文件...")

    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            print(f"[信息] 找到 {len(csv_files)} 个 CSV 文件")

            for csv_file in csv_files:
                filename = os.path.basename(csv_file)
                print(f"[信息] 提取: {filename}")
                with zip_ref.open(csv_file) as source:
                    target_path = os.path.join(TARGET_DIR, filename)
                    with open(target_path, 'wb') as target:
                        target.write(source.read())

        print()
        print("[成功] 数据集已提取到 dataset/ 目录")
        print()

        print("文件列表:")
        for f in os.listdir(TARGET_DIR):
            if f.endswith('.csv'):
                size_mb = os.path.getsize(os.path.join(TARGET_DIR, f)) / 1024 / 1024
                print(f"  - {f}: {size_mb:.2f} MB")

    except Exception as e:
        print(f"[错误] 解压失败: {e}")
        sys.exit(1)

    print()
    print("=" * 60)
    print("完成！现在可以运行流水线脚本了。")
    print("=" * 60)

if __name__ == '__main__':
    download_movielens()
