-- 仅部署报告 #19，避免覆盖其他离线分析报告。
SET NAMES utf8mb4;

START TRANSACTION;

DELETE FROM blog WHERE bindex = 19;

INSERT INTO blog (bindex, btype, paragraph, content, is_realtime) VALUES
(19, 1, 0, 'LuckyAnJun #19 Blog 实时 5 分钟窗口监控。历史 UserBehavior 日志逐条回放到 Kafka，Spark Structured Streaming 按事件时间持续计算最近窗口的 PV、收藏、加购、购买和异常等级，并写入 MySQL。Blog 图表块每 1 秒重新查询，无需单独实时页面。', 0),
(19, 1, 1, '实时指标说明：PV 反映浏览流量，收藏表示兴趣，加购表示较强购买意向，购买表示最终行为次数；alert_level 将预警直观量化为 0=正常、1=低流量或低转化提醒、2=流量突增或骤降。图中只保留最近 12 个窗口，新窗口会从右侧持续出现。', 0),
(19, 0, 2, 'lb_realtime_blog_metrics(window_label,pv,fav_cnt,cart_cnt,buy_cnt,alert_level)#mix', 1),
(19, 1, 3, '综合结论：Kafka 输入、Spark micro-batch 聚合、MySQL 最近窗口表和 Blog 每秒刷新构成实时闭环。答辩时启动回放后，可直接在报告 #19 看到窗口推进、行为指标变化和预警等级跳变；这是历史日志实时回放模拟，不代表淘宝官方实时流量。', 0);

COMMIT;
