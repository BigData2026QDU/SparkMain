# SparkMain

## 简介

SparkMain是一个基于Spark的大数据处理项目，使用MovieLens电影评分数据集进行分析。

## 功能特性

- 自动化数据清洗流水线
- 支持Hive集成
- 跨平台支持（Windows和Linux）
- 5个不同的Spark分析任务

## 环境要求

- JDK 17
- Apache Spark
- Hive
- Python 3.x

## 数据集

使用MovieLens 25M数据集：
- `movies.csv` - 电影信息（movieId, title, genres）
- `ratings.csv` - 评分数据（userId, movieId, rating, timestamp）
- `tags.csv` - 标签数据（userId, movieId, tag, timestamp）
- `links.csv` - 链接数据（movieId, imdbId, tmdbId）

下载地址：https://grouplens.org/datasets/movielens/25m/

## Spark分析任务

### 任务1: 基础聚合分析
- 统计每部电影的平均评分、评分次数、最高分、最低分
- 使用GROUP BY和聚合函数

### 任务2: 窗口函数分析
- 计算电影在各类型中的排名、评分百分位
- 使用ROW_NUMBER, RANK, PERCENT_RANK

### 任务3: 类型分析
- 分析各类型的电影数量、平均评分、最受欢迎类型
- 使用LATERAL VIEW和字符串处理

### 任务4: 时间分析
- 分析评分的时间分布、年度趋势、活跃时段
- 使用日期函数和时间窗口分析

### 任务5: 用户行为分析
- 分析用户评分行为、活跃度、偏好类型
- 使用复杂子查询和用户画像分析

## 快速开始

### 1. 克隆仓库

```bash
git clone https://github.com/BigData2026QDU/SparkMain.git
cd SparkMain
git submodule update --init --recursive
```

### 2. 下载数据集

从 https://grouplens.org/datasets/movielens/25m/ 下载数据集，将CSV文件放入 `dataset/` 目录。

### 3. 配置环境

确保以下命令可用：
- `hive`
- `hdfs`
- `python3`

### 4. 运行流水线

**Linux/Mac:**
```bash
chmod +x main_pipeline.sh
./main_pipeline.sh
```

**Windows:**
```cmd
main_pipeline.bat
```

## 项目结构

```
SparkMain/
├── SparkMain/              # 源代码目录
│   ├── src/                # 源代码
│   └── test/               # 测试代码
├── AGENTS/                 # 项目规范（submodule）
├── dataset/                # 原始数据目录
├── cleanPy/                # Python清洗脚本
├── initializeSQL/          # Hive建表SQL
├── prepareData/            # 数据准备SQL
├── jobSQL/                 # 分析任务SQL
├── Architecture.md         # 架构文档
├── README.md               # 项目说明
├── File_Index.md           # 文件索引
├── main_pipeline.sh        # Linux流水线脚本
├── main_pipeline.bat       # Windows流水线脚本
├── truncate_file.py        # 数据截断工具
└── .gitignore              # Git忽略配置
```

## 开发指南

1. 遵循AGENTS仓库中的规范文档
2. 代码变更时同步更新文档
3. 提交前检查规范遵守情况

## 贡献指南

1. Fork 本仓库
2. 新建 Feat_xxx 分支
3. 提交代码
4. 新建 Pull Request

## 许可证

本项目遵循项目规范仓库中的许可证要求。
