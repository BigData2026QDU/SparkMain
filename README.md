# SparkMain

## 简介

SparkMain 是一个基于 Apache Spark + Scala 的大数据处理流水线，使用 MovieLens 电影评分数据集进行分析。本项目提供自动化数据清洗、分析任务执行的完整流程。

## 环境要求

| 组件 | 版本 | 说明 |
|------|------|------|
| JDK | 17 | Java 开发工具包 |
| Scala | 2.12 | Scala 开发工具包 |
| Apache Spark | 3.5.0 | 大数据处理框架 |
| sbt | 1.9+ | Scala 构建工具 |
| Python | 3.8+ | 数据清洗脚本 |
| Kafka | 3.6+ | 消息队列（实时流处理） |

## 快速开始

### 1. 克隆仓库

```bash
git clone https://github.com/BigData2026QDU/SparkMain.git
cd SparkMain
git submodule update --init --recursive
```

### 2. 下载数据集

从 [MovieLens 25M](https://grouplens.org/datasets/movielens/25m/) 下载数据集，将 CSV 文件放入 `dataset/` 目录。

或使用内置下载脚本：

```bash
python dataset/download_movielens.py
```

### 3. 构建项目

```bash
cd SparkMain
sbt package
```

### 4. 运行流水线

**生产模式：**

```bash
chmod +x main_pipeline_new.sh
./main_pipeline_new.sh
```

**测试模式：**

```bash
chmod +x main_pipeline_test.sh
./main_pipeline_test.sh
```

## 流水线说明

### 执行流程

```
原始数据 (dataset/)
    ↓
Python 清洗脚本 (cleanPy/)
    ↓
清洗后数据 (cleanedDataset/)
    ↓
Spark 分析任务 (Scala)
    ↓
分析结果 (output/)
```

### 目录结构

```
SparkMain/
├── SparkMain/              # 源代码目录
│   ├── src/main/scala/org/example/
│   │   ├── Main.scala              # 主入口
│   │   ├── streaming/
│   │   │   ├── RatingStreamProcessor.scala  # Spark Streaming 处理器
│   │   │   └── RatingProducer.scala         # Kafka 数据生成器
│   │   └── analysis/
│   │       ├── AnalyzeRatings.scala         # 评分分析
│   │       └── AnalyzeGenres.scala          # 类型分析
│   ├── build.sbt           # Scala 构建配置
│   └── test/
├── dataset/                # 原始数据目录
├── dataset_test/           # 测试数据目录（轻量级）
├── cleanPy/                # 生产清洗脚本
├── cleanPy_test/           # 测试清洗脚本
├── output/                 # 分析结果输出
├── config/                 # 配置文件
├── start_streaming.sh      # 启动 Kafka + Spark Streaming
├── main_pipeline_new.sh    # 新流水线脚本（Spark + Scala）
├── main_pipeline_test.sh   # 测试流水线脚本
├── Architecture.md         # 架构文档
├── README.md               # 项目说明
├── Workflow.md             # 工作流设计文档
└── .gitignore              # Git 忽略配置
```

## 如何编写新任务

### 任务规范

在 `SparkMain/src/main/scala/org/example/analysis/` 目录下创建 Scala 文件：

1. **文件命名：** `Analyze任务名称.scala`
   - 示例：`AnalyzeMovies.scala`

2. **Scala 代码模板：**
```scala
package org.example.analysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AnalyzeMovies {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AnalyzeMovies")
      .getOrCreate()

    // 读取数据
    val data = spark.read.parquet("output/ratings_streaming")

    // 分析逻辑
    val result = data.groupBy("movieId")
      .agg(avg("rating").as("avg_rating"))

    // 输出结果
    result.show()

    // 保存结果
    result.write.mode("overwrite").parquet("output/movie_stats")

    spark.stop()
  }
}
```

3. **在 Main.scala 中注册任务：**
```scala
case "movies" => AnalyzeMovies.main(args.drop(1))
```

4. **运行任务：**
```bash
spark-submit --class org.example.Main movies SparkMain/target/scala-2.12/sparkmain_2.12-1.0.jar
```

### 测试模式

本项目支持测试模式，使用轻量级测试数据（10-100KB）快速验证流水线。

**测试目录结构：**

| 生产目录 | 测试目录 | 说明 |
|---------|---------|------|
| `dataset/` | `dataset_test/` | 测试数据 |
| `cleanPy/` | `cleanPy_test/` | 测试清洗脚本 |

**运行测试：**

```bash
./main_pipeline_test.sh
```

## 实时流处理

本项目支持基于 Kafka + Spark Structured Streaming 的增量数据处理。

### 架构

```
Kafka Topic (ratings)
    ↓
Spark Structured Streaming
    ↓
Parquet 文件 (output/ratings_streaming)
```

### 启动服务

```bash
# 启动 Kafka + Spark Streaming
chmod +x start_streaming.sh
./start_streaming.sh
```

### 生成测试数据

```bash
# 编译项目
cd SparkMain
sbt package

# 运行数据生成器
spark-submit --class org.example.streaming.RatingProducer target/scala-2.12/sparkmain_2.12-1.0.jar
```

## 项目结构

```
SparkMain/
├── SparkMain/              # 源代码目录
│   ├── src/main/scala/org/example/
│   │   ├── Main.scala
│   │   ├── streaming/
│   │   │   ├── RatingStreamProcessor.scala
│   │   │   └── RatingProducer.scala
│   │   └── analysis/
│   │       ├── AnalyzeRatings.scala
│   │       └── AnalyzeGenres.scala
│   ├── build.sbt
│   └── test/
├── config/
│   └── streaming.properties
├── dataset/
├── dataset_test/
├── cleanPy/
├── cleanPy_test/
├── output/
├── start_streaming.sh
├── main_pipeline_new.sh
├── main_pipeline_test.sh
├── Architecture.md
├── README.md
├── Workflow.md
└── .gitignore
```

## 贡献指南

1. Fork 本仓库
2. 新建 `feature/xxx` 或 `hotfix/xxx` 分支
3. 按照「如何编写新任务」规范添加 Scala 代码
4. 运行 `sbt compile` 确保编译通过
5. 本地测试流水线执行通过
6. 提交代码并创建 Pull Request

## 许可证

本项目遵循项目规范仓库中的许可证要求。
