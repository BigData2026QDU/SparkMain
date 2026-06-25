# SparkMain

## 简介

SparkMain 是一个基于 Apache Spark 的大数据处理流水线，使用 MovieLens 电影评分数据集进行分析。本项目提供自动化数据清洗、Hive 建表、数据加载和分析任务执行的完整流程。

## 环境要求

| 组件 | 版本 | 说明 |
|------|------|------|
| JDK | 17 | Java 开发工具包 |
| Apache Spark | 3.5.0 | 大数据处理框架 |
| Hive | 3.1.x | 数据仓库 |
| Hadoop (HDFS) | - | 分布式文件系统 |
| Python | 3.8+ | 数据清洗脚本 |

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

### 3. 运行流水线

**生产模式：**

Linux/Mac：
```bash
chmod +x main_pipeline.sh
./main_pipeline.sh
```

Windows：
```cmd
main_pipeline.bat
```

**测试模式：**

Linux/Mac：
```bash
chmod +x main_pipeline_test.sh
./main_pipeline_test.sh
```

Windows：
```cmd
main_pipeline_test.bat
```

## 流水线说明

### 执行流程

流水线自动执行以下 8 个步骤：

| 步骤 | 操作 | 说明 |
|------|------|------|
| 1 | 检查/创建 Hive 数据库 | 数据库 `bigdata_ana` 不存在则自动创建 |
| 2 | 截断大文件 | `truncate_file.py` 将 CSV 截断至 300MB |
| 3 | 运行清洗脚本 | 执行 `cleanPy/` 目录下所有 Python 脚本 |
| 4 | 上传至 HDFS | 清理旧数据并上传清洗后的 CSV |
| 5 | 初始化 Hive 表 | 检查表是否存在，不存在则执行 `initializeSQL/` |
| 6 | 数据准备 | 执行 `prepareData/` 中的 SQL（创建视图等） |
| 7 | 执行分析任务 | 执行 `jobSQL/` 目录下所有 SQL 文件 |
| 8 | 完成标记 | 打印流水线执行完成信息 |

### 目录结构

```
SparkMain/
├── dataset/            # 生产数据（MovieLens CSV 文件）
├── dataset_test/       # 测试数据（轻量级，10-100KB）
├── truncatedDataset/   # 截断后的数据（自动生成）
├── cleanedDataset/     # 清洗后的数据（自动生成）
├── cleanPy/            # 生产清洗脚本
├── cleanPy_test/       # 测试清洗脚本
├── initializeSQL/      # 生产建表 SQL
├── initializeSQL_test/ # 测试建表 SQL
├── prepareData/        # 生产数据准备 SQL
├── prepareData_test/   # 测试数据准备 SQL
├── jobSQL/             # 生产分析任务 SQL
├── jobSQL_test/        # 测试分析任务 SQL
├── main_pipeline.sh    # 生产流水线脚本（Linux）
├── main_pipeline.bat   # 生产流水线脚本（Windows）
├── main_pipeline_test.sh  # 测试流水线脚本（Linux）
├── main_pipeline_test.bat # 测试流水线脚本（Windows）
├── truncate_file.py    # 大文件截断工具
└── test/               # 测试验证脚本
```

## 如何编写新任务

### 任务规范

在 `jobSQL/` 目录下创建 SQL 文件，遵循以下规范：

1. **文件命名：** `XX_任务名称简述.sql`（XX 为两位数字编号，决定执行顺序）
   - 示例：`06_task6_hot_movies.sql`

2. **SQL 文件必须包含：**
   ```sql
   SET hive.execution.engine=spark;
   SET spark.master=local[*];
   
   USE bigdata_ana;
   ```

3. **结果表命名：** `taskN_xxx`（N 为任务编号）
   - 示例：`task6_hot_movies`

4. **SQL 模式：**
   ```sql
   SET hive.execution.engine=spark;
   SET spark.master=local[*];
   
   USE bigdata_ana;
   
   -- 删除旧结果表（如存在）
   DROP TABLE IF EXISTS taskN_xxx;
   
   -- 创建结果表
   CREATE TABLE taskN_xxx AS
   SELECT ...
   FROM ...
   WHERE ...;
   
   -- 展示结果
   SELECT * FROM taskN_xxx LIMIT 20;
   ```

### 测试模式

本项目支持测试模式，使用轻量级测试数据（10-100KB）快速验证流水线。

**测试目录结构：**

| 生产目录 | 测试目录 | 说明 |
|---------|---------|------|
| `dataset/` | `dataset_test/` | 测试数据 |
| `cleanPy/` | `cleanPy_test/` | 测试清洗脚本 |
| `initializeSQL/` | `initializeSQL_test/` | 测试建表 SQL |
| `prepareData/` | `prepareData_test/` | 测试数据准备 SQL |
| `jobSQL/` | `jobSQL_test/` | 测试分析任务 SQL |

**编写测试任务：**

1. 在 `jobSQL_test/` 目录下创建测试 SQL 文件
2. 使用 `bigdata_ana_test` 数据库
3. 运行测试流水线验证：

```bash
# Linux/Mac
./main_pipeline_test.sh

# Windows
main_pipeline_test.bat
```

4. 运行验证脚本检查结果：

```bash
bash test/verify_results.sh
```

### 可用数据表

流水线初始化后，`bigdata_ana` 数据库中包含以下表：

| 表名 | 字段 | 说明 |
|------|------|------|
| `movies` | movieId, title, genres | 电影信息 |
| `ratings` | userId, movieId, rating, timestamp | 评分数据 |
| `tags` | userId, movieId, tag, timestamp | 标签数据 |
| `links` | movieId, imdbId, tmdbId | 外部链接 |

流水线还会创建临时视图 `v_movies_ratings`（JOIN movies 和 ratings）。

### 提交要求

1. SQL 文件放在 `jobSQL/` 目录
2. 遵循文件命名规范（`XX_taskname.sql`）
3. 文件开头包含 Spark 引擎设置和 `USE bigdata_ana`
4. 结果表使用 `taskN_xxx` 命名格式
5. 本地测试通过后提交

## 项目结构

```
SparkMain/
├── SparkMain/              # 源代码目录
│   ├── src/                # 源代码
│   └── test/               # 测试代码
├── AGENTS/                 # 项目规范（submodule）
├── dataset/                # 生产数据目录
├── dataset_test/           # 测试数据目录（轻量级）
├── cleanPy/                # 生产清洗脚本
├── cleanPy_test/           # 测试清洗脚本
├── initializeSQL/          # 生产建表 SQL
├── initializeSQL_test/     # 测试建表 SQL
├── prepareData/            # 生产数据准备 SQL
├── prepareData_test/       # 测试数据准备 SQL
├── jobSQL/                 # 生产分析任务 SQL
├── jobSQL_test/            # 测试分析任务 SQL
├── test/                   # 测试验证脚本
├── Architecture.md         # 架构文档
├── README.md               # 项目说明
├── File_Index.md           # 文件索引
├── main_pipeline.sh        # 生产流水线脚本（Linux）
├── main_pipeline.bat       # 生产流水线脚本（Windows）
├── main_pipeline_test.sh   # 测试流水线脚本（Linux）
├── main_pipeline_test.bat  # 测试流水线脚本（Windows）
├── truncate_file.py        # 数据截断工具
└── .gitignore              # Git 忽略配置
```

## 贡献指南

1. Fork 本仓库
2. 新建 `feature/xxx` 或 `hotfix/xxx` 分支
3. 按照「如何编写新任务」规范添加 SQL 文件
4. 本地测试流水线执行通过
5. 提交代码并创建 Pull Request

## 许可证

本项目遵循项目规范仓库中的许可证要求。
