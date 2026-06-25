"""
测试数据清洗脚本 - 清洗评分数据
输入: dataset_test/ratings.csv
输出: cleanedDataset_test/ratings.csv
"""

import csv
import os

INPUT_DIR = "dataset_test"
OUTPUT_DIR = "cleanedDataset_test"
INPUT_FILE = os.path.join(INPUT_DIR, "ratings.csv")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "ratings.csv")

def clean_ratings():
    """清洗评分数据"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    total = 0
    valid = 0
    skipped = 0
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as infile, \
         open(OUTPUT_FILE, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        header = next(reader)
        writer.writerow(header)
        
        for row in reader:
            total += 1
            try:
                if len(row) != 4:
                    skipped += 1
                    continue
                
                user_id = int(row[0])
                movie_id = int(row[1])
                rating = float(row[2])
                timestamp = int(row[3])
                
                if rating < 0.5 or rating > 5.0:
                    skipped += 1
                    continue
                
                writer.writerow([user_id, movie_id, rating, timestamp])
                valid += 1
                
            except (ValueError, IndexError):
                skipped += 1
                continue
    
    print(f"评分数据清洗完成:")
    print(f"  总行数: {total}")
    print(f"  有效行数: {valid}")
    print(f"  跳过行数: {skipped}")

if __name__ == "__main__":
    clean_ratings()
