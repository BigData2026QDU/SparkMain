# 系统架构

## 1. 整体架构

本项目为Spark大数据处理项目，主要负责数据清洗、转换和分析。

## 2. 核心模块

### 2.1 数据采集模块
- 负责从数据源采集原始数据
- 支持多种数据源格式

### 2.2 数据处理模块
- 数据清洗和转换
- 数据验证和质量检查

### 2.3 数据分析模块
- 基于Spark的数据分析
- 支持Hive集成

## 3. 技术选型

- Java 17 (JDK 17)
- Apache Spark
- Hive
- Python (数据清洗脚本)

## 4. 数据流向

```
原始数据 → 数据清洗 → HDFS存储 → Hive分析 → 结果输出
```

LuckyAnJun 淘宝用户行为个人分析数据流:

```
UserBehavior.csv → 清洗与时间字段补充 → HDFS表目录 → Hive外部表 → Spark SQL离线报表
历史日志回放 → Kafka taobao_behavior → Spark Structured Streaming → 5分钟窗口指标
```

## 5. 部署架构

- 本地开发环境
- Hadoop集群环境
