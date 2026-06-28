# SparkMain 运行包

本压缩包提供已构建的 SparkMain 应用 JAR、运行脚本、示例配置和必要的本地脚本。使用者不需要安装 sbt，也不需要仓库源码。

## 环境要求

- JDK 17
- Scala 2.12 运行环境
- Apache Spark 3.5.x，`spark-submit` 已加入 `PATH`
- Kafka 3.6.x（运行实时任务时需要）
- 本地环境文件：`conf/sparkmain.env` 或 `.env`

完整批处理脚本 `main_pipeline_new.sh` 会执行 Python 数据下载/清洗辅助脚本；如果需要从原始 MovieLens CSV 开始跑完整流水线，本机还需要 Python 3.8+。

## 本地配置

复制示例配置，然后只在本机填写真实值：

```bash
cp conf/sparkmain-env.example conf/sparkmain.env
```

`conf/sparkmain.env`、`.env` 和其他真实 env 文件只用于本地运行，不要上传到 Git、CI 日志或截图。

env 文件只支持 SparkMain 已知配置项的 `KEY=VALUE` 行；空行和 `#` 注释会被忽略，其他 shell 语法会被跳过且不会执行。

默认不会写入 MySQL：

```bash
MYSQL_ENABLED=false
OFFLINE_MYSQL_ENABLED=false
```

只有确认要写入自己的数据库时，才在本地 env 文件中改成 `true` 并填写 JDBC URL、用户、密码和表名。

## 运行

查看帮助：

```bash
bash bin/sparkmain help
```

运行单个离线报告：

```bash
bash bin/sparkmain ratings dataset output/ratings
bash bin/sparkmain genres dataset output/genres
bash bin/sparkmain time dataset output/time
bash bin/sparkmain users dataset output/users
```

运行实时统计：

```bash
bash bin/sparkmain stream
```

从新解压的运行包执行完整批处理前，需要先准备 MovieLens CSV。运行包不会内置大数据集；请选择其中一种方式：

```bash
python dataset/download_movielens.py
```

或者手动把 MovieLens 的 `ratings.csv` 和 `movies.csv` 放入 `dataset/`。

只想快速验证 Spark 入口时，可以使用包内小型测试数据运行单个报告：

```bash
bash bin/sparkmain ratings dataset_test output/ratings_smoke
```

数据准备完成后再运行完整批处理流水线：

```bash
bash bin/sparkmain batch
```

也可以手动指定 JAR：

```bash
SPARKMAIN_JAR=lib/sparkmain_2.12-<version>.jar bash bin/sparkmain help
```

脚本会自动加载第一个存在的本地 env 文件：`.env`、`sparkmain.env` 或 `conf/sparkmain.env`。日志只显示是否加载了配置文件和 MySQL 是否启用，不输出密码或完整 JDBC URL。
