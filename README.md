# SparkMain

## 简介

SparkMain是一个基于Spark的大数据处理项目，提供完整的数据清洗、转换和分析流水线。

## 功能特性

- 自动化数据清洗流水线
- 支持Hive集成
- 跨平台支持（Windows和Linux）
- 完整的文档和规范

## 环境要求

- JDK 17
- Apache Spark
- Hive
- Python 3.x

## 快速开始

### 1. 克隆仓库

```bash
git clone https://github.com/BigData2026QDU/SparkMain.git
cd SparkMain
git submodule update --init --recursive
```

### 2. 配置环境

确保以下命令可用：
- `hive`
- `hdfs`
- `python3`

### 3. 运行流水线

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
├── Architecture.md         # 架构文档
├── README.md               # 项目说明
├── File_Index.md           # 文件索引
├── main_pipeline.sh        # Linux流水线脚本
├── main_pipeline.bat       # Windows流水线脚本
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