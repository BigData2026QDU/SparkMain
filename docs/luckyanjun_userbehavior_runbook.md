# LuckyAnJun UserBehavior 运行手册

## 环境

最终任务在 CentOS 虚拟机执行，当前验收环境为：

- Hadoop 2.9.2
- Hive 2.3.7
- Spark 2.4.6
- Scala 2.11.12
- Kafka 2.5.0
- MySQL 5.7

Windows 用于编辑代码、GitHub 管理和访问展示页面。

## 离线任务

将淘宝 `UserBehavior.csv` 放入 `dataset/`，然后在仓库根目录执行：

```bash
HIVE_EXECUTION_ENGINE=mr bash main_pipeline.sh
```

Scala 分析作业位于
`SparkMain/src/main/java/org/example/analysis/`，对应 issue #14 至 #18。
生产结果写入 Hive 数据库 `bigdata_ana`。

主要结果表：

- `lb_funnel_item_path_stage`
- `lb_time_hour_distribution`
- `lb_category_topn`
- `lb_user_active_day_distribution`

## 实时任务

实时任务对应 issue #19。它把历史日志逐条回放到 Kafka，并保留原始事件
时间，因此答辩时应表述为“历史日志实时回放模拟”，不能表述为接入淘宝官方
实时数据。

### 1. 配置 MySQL

```bash
export MYSQL_USER=root
export MYSQL_PASSWORD='你的密码'
```

可选配置：

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_USER_BEHAVIOR_TOPIC=taobao_behavior
export USER_BEHAVIOR_CHECKPOINT=/tmp/spark/checkpoints/luckyanjun_user_behavior
export REALTIME_WEB_PORT=18080
```

### 2. 启动实时分析

```bash
bash start_user_behavior_streaming.sh
```

脚本会启动 ZooKeeper 和 Kafka、创建 topic 和 MySQL 表、编译 Scala 作业、
启动动态页面服务，然后运行 Spark Structured Streaming。Spark 2.4.6 所需
Kafka connector 由 `spark-submit --packages` 加载。

### 3. 回放数据

另开一个终端执行：

```bash
bash replay_user_behavior.sh \
  dataset_test/UserBehavior.csv \
  taobao_behavior \
  localhost:9092 \
  200
```

第 4 个参数是每条消息之间的毫秒延迟。可添加第 5 个参数限制回放行数；
`0` 表示回放全部数据。

### 4. 查看结果

MySQL 表：

- `lb_realtime_window_metrics`

页面：

```text
http://192.168.211.101:18080/luckyanjun_realtime.html
```

页面每 5 秒读取 Spark 更新的 `web/data/realtime.json`，展示 5 分钟窗口内的
PV、收藏、加购、购买和异常预警。

## 共享 MySQL

虚拟机不能直接访问公网 MySQL 时，先在 Windows 建立反向隧道：

```powershell
ssh -N -R 13306:47.104.27.184:3306 master@192.168.211.101
```

虚拟机通过 `127.0.0.1:13306/test_db` 访问共享数据库。密码只保存在权限为
`600` 的 `/home/master/.sparkmain_mysql.env`，不能提交到 Git。

导出 4 张离线展示表：

```bash
bash export_user_behavior_mysql.sh
```

启动写入共享数据库的实时任务：

```bash
bash start_user_behavior_streaming_remote.sh
```

共享表契约见 `docs/luckyanjun_mysql_contract.md`。

### 5. 重新验收

需要从头重新处理时，使用新的 checkpoint 路径，或在确认实时任务已经停止后
删除旧 checkpoint。MySQL 的 `lb_realtime_batches` 使用 checkpoint 和
batch ID 保证重试幂等。

## 指标边界

- `buy` 表示购买行为次数，不表示订单金额、销售额或 GMV。
- PV、收藏、加购、购买均为窗口内行为次数。
- 低转化按购买次数 / PV 小于 1% 判断。
- 异常规则覆盖流量突增、流量骤降、低流量和低转化。

## 测试

轻量数据位于 `dataset_test/UserBehavior.csv`。静态聚合测试：

```bash
bash test_user_behavior_realtime.sh
```

静态聚合测试和集成验收都必须在虚拟机执行；集成验收还需实际启动 Kafka、
Spark 和 MySQL，并查询 `lb_realtime_window_metrics`。
