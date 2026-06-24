#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
大文件截断工具
功能：将大CSV文件截断到指定大小，用于测试和开发
"""

import sys
import os
import argparse

DEFAULT_TARGET_SIZE_MB = 300

def truncate_file(input_file, output_file, target_size_mb=DEFAULT_TARGET_SIZE_MB):
    """
    截断文件到指定大小
    
    Args:
        input_file: 输入文件路径
        output_file: 输出文件路径
        target_size_mb: 目标大小（MB）
    """
    target_size = target_size_mb * 1024 * 1024
    
    if not os.path.exists(input_file):
        print(f"[错误] 输入文件不存在: {input_file}")
        sys.exit(1)
    
    file_size = os.path.getsize(input_file)
    print(f"\n输入文件: {input_file}")
    print(f"文件大小: {file_size / 1024 / 1024:.2f} MB")
    print(f"目标大小: {target_size_mb} MB")
    
    if file_size <= target_size:
        print(f"\n文件大小已在目标范围内，直接复制")
        import shutil
        shutil.copy2(input_file, output_file)
        return
    
    total_bytes = 0
    lines_read = 0
    
    try:
        with open(input_file, 'r', encoding='utf-8') as in_f:
            with open(output_file, 'w', encoding='utf-8') as out_f:
                header = in_f.readline()
                out_f.write(header)
                total_bytes += len(header.encode('utf-8'))
                
                for line in in_f:
                    line_size = len(line.encode('utf-8'))
                    
                    if total_bytes + line_size > target_size:
                        if total_bytes == 0:
                            out_f.write(line)
                            total_bytes += line_size
                            lines_read += 1
                            print(f"\n首行数据: {lines_read} 行 -> {line_size / 1024 / 1024:.2f} MB")
                            print(f"  超出目标大小 {target_size_mb} MB")
                            print(f"  使用 --size 调整目标大小")
                        else:
                            print(f"\n已截断: {lines_read} 行 -> {total_bytes / 1024 / 1024:.2f} MB")
                        break
                    
                    out_f.write(line)
                    total_bytes += line_size
                    lines_read += 1
                    
                    if lines_read % 10000 == 0:
                        print(f"  已处理: {lines_read} 行 -> {total_bytes / 1024 / 1024:.2f} MB")
        
        print(f"\n完成! 输出文件: {output_file}")
        print(f"  行数: {lines_read}")
        print(f"  大小: {total_bytes / 1024 / 1024:.2f} MB")
        print(f"  目标: {target_size_mb} MB")
        print(f"  完成度: {total_bytes / target_size * 100:.1f}%")
        
    except IOError as e:
        print(f"[错误] 文件操作失败: {e}")
        sys.exit(1)

def process_dataset_directory(dataset_dir='dataset', output_dir='truncatedDataset', target_size_mb=DEFAULT_TARGET_SIZE_MB):
    """
    处理 dataset 目录下的所有 CSV 文件
    
    Args:
        dataset_dir: 输入数据目录
        output_dir: 输出数据目录
        target_size_mb: 目标大小（MB）
    """
    if not os.path.exists(dataset_dir):
        print(f"[错误] 数据目录不存在: {dataset_dir}")
        sys.exit(1)
    
    os.makedirs(output_dir, exist_ok=True)
    
    csv_files = [f for f in os.listdir(dataset_dir) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"[警告] {dataset_dir} 目录下没有找到 CSV 文件")
        return
    
    print(f"\n找到 {len(csv_files)} 个 CSV 文件:")
    for f in csv_files:
        print(f"  - {f}")
    
    for csv_file in csv_files:
        input_path = os.path.join(dataset_dir, csv_file)
        output_path = os.path.join(output_dir, csv_file)
        
        print(f"\n处理: {csv_file}")
        truncate_file(input_path, output_path, target_size_mb)
    
    print(f"\n全部完成! 输出目录: {output_dir}")

def main():
    parser = argparse.ArgumentParser(
        description='大文件截断工具 - 将大CSV文件截断到指定大小'
    )
    
    parser.add_argument('input_file', nargs='?', help='输入文件路径（可选，默认处理 dataset 目录）')
    parser.add_argument('-o', '--output', help='输出文件路径（默认: output_<input>）')
    parser.add_argument('--size', type=int, default=DEFAULT_TARGET_SIZE_MB,
                        help=f'目标大小MB（默认: {DEFAULT_TARGET_SIZE_MB}）')
    parser.add_argument('--status', action='store_true', help='显示文件状态')
    parser.add_argument('--reset', action='store_true', help='重置处理进度')
    parser.add_argument('--dataset-dir', default='dataset', help='数据集目录（默认: dataset）')
    parser.add_argument('--output-dir', default='truncatedDataset', help='输出目录（默认: truncatedDataset）')
    
    args = parser.parse_args()
    
    if args.status:
        if args.input_file:
            if os.path.exists(args.input_file):
                file_size = os.path.getsize(args.input_file)
                print(f"\n文件状态: {args.input_file}")
                print(f"  大小: {file_size / 1024 / 1024:.2f} MB")
                
                with open(args.input_file, 'r', encoding='utf-8') as f:
                    total_lines = sum(1 for _ in f)
                    print(f"  行数: {total_lines}")
            else:
                print(f"文件不存在: {args.input_file}")
        else:
            if os.path.exists(args.dataset_dir):
                csv_files = [f for f in os.listdir(args.dataset_dir) if f.endswith('.csv')]
                print(f"\n数据集目录: {args.dataset_dir}")
                print(f"  CSV 文件数: {len(csv_files)}")
                for f in csv_files:
                    file_path = os.path.join(args.dataset_dir, f)
                    file_size = os.path.getsize(file_path)
                    print(f"  - {f}: {file_size / 1024 / 1024:.2f} MB")
            else:
                print(f"数据集目录不存在: {args.dataset_dir}")
        return
    
    if args.reset:
        if args.input_file:
            print(f"重置: {args.input_file}")
        else:
            print(f"重置数据集目录: {args.dataset_dir}")
        return
    
    if args.input_file:
        if not args.output:
            base_name = os.path.basename(args.input_file)
            args.output = os.path.join(args.output_dir, base_name)
        
        os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
        truncate_file(args.input_file, args.output, args.size)
    else:
        process_dataset_directory(args.dataset_dir, args.output_dir, args.size)

if __name__ == '__main__':
    main()
