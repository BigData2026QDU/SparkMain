# 增量大数据分析工作流

## 1. 仓库决策

**不新建仓库**，在 SparkMain 中实现。

## 2. 核心需求

- **增量数据：** 新数据不断产生，只需处理新增部分
- **动态数据：** 数据内容会变化（更新、删除）
- **实时性：** 数据变化后能快速反映到分析结果

## 3. 技术栈

| 层次 | 技术 |
|------|------|
| 批处理 | Spark SQL |
| 实时处理 | Spark Structured Streaming |
| 消息队列 | Apache Kafka |
| 数据存储 | Parquet 文件 |

## 4. 工作流

### 4.1 数据采集

```
数据源 → Kafka Topic → Spark Structured Streaming → Parquet 文件
```

### 4.2 增量处理

```
检测新数据 → 读取时间戳 → 过滤新增数据 → 合并到结果表
```

### 4.3 分析任务

```
读取 Parquet → Spark SQL 分析 → 输出结果
```

## 5. 文件结构

```
SparkMain/
├── SparkMain/src/main/scala/org/example/
│   ├── streaming/
│   │   ├── RatingStreamProcessor.scala
│   │   └── RatingProducer.scala
│   └── analysis/
│       ├── AnalyzeRatings.scala
│       └── AnalyzeGenres.scala
├── config/
│   └── streaming.properties
├── start_streaming.sh
└── main_pipeline.sh
```

## 6. 实施步骤

1. 创建 Scala 项目结构
2. 实现 Spark Streaming 处理器
3. 实现 Kafka 数据生成器
4. 实现分析任务
5. 更新流水线脚本
6. 更新文档
