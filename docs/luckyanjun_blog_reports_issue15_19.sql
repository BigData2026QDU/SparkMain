SET NAMES utf8mb4;

DROP TABLE IF EXISTS lb_funnel_overall_stage;
CREATE TABLE lb_funnel_overall_stage (
  stage_order INT NOT NULL,
  stage_name VARCHAR(32) NOT NULL,
  user_cnt BIGINT NOT NULL,
  conversion_rate DOUBLE DEFAULT NULL,
  PRIMARY KEY (stage_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO lb_funnel_overall_stage (stage_order, stage_name, user_cnt, conversion_rate)
SELECT 1, '浏览用户', pv_users, 1.0 FROM lb_funnel_overall
UNION ALL
SELECT 2, '意向用户', intent_users, pv_to_intent_rate FROM lb_funnel_overall
UNION ALL
SELECT 3, '购买用户', buy_users, pv_to_buy_rate FROM lb_funnel_overall;

START TRANSACTION;

DELETE FROM blog WHERE bindex IN (15, 16, 17, 18, 19);

INSERT INTO blog (bindex, btype, paragraph, content) VALUES
(15, 1, 0, 'LuckyAnJun #15 用户行为整体漏斗概况。本报告基于淘宝 UserBehavior 清洗后的明细表，由 Spark 离线任务计算并导出到 MySQL。分析口径为：pv 表示浏览，fav 和 cart 合并为意向行为，buy 表示购买行为；该漏斗用于观察用户是否曾到达某个行为阶段，不假设严格的 pv -> fav/cart -> buy 顺序。'),
(15, 1, 1, '一、整体漏斗概况。当前结果中，发生过浏览的用户为 55611 人，发生过收藏或加购意向的用户为 48831 人，发生过购买的用户为 38019 人。浏览用户中同时发生意向行为的用户为 48605 人，转化率为 87.40%；浏览用户中最终购买的用户为 37836 人，转化率为 68.04%。图表横轴按浏览用户、意向用户、购买用户三个阶段展示，便于直观看到用户规模逐级收窄。'),
(15, 0, 2, 'lb_funnel_overall_stage(stage_name,user_cnt)#bar'),
(15, 1, 3, '综合结论：#15 报告只保留整体漏斗概况，用于展示浏览、意向和购买三个阶段的总体用户规模与总体转化关系。图表只画三个阶段的用户数，转化率在文字中说明，避免把多个宽表字段并列画成难以理解的柱状图。'),

(16, 1, 0, 'LuckyAnJun #16 逐小时流量与购买比例分析。本报告只保留 lb_time_hour_distribution 这一张逐小时结果表，横轴按 0 点到 23 点顺序展示。图表只展示 PV 和 hourly_buy_rate，避免多个指标混在一起导致报表阅读混乱。'),
(16, 1, 1, '一、逐小时概况。按 24 小时聚合后，21 点 PV 最高，为 431141；22 点为 422791；20 点为 376042，晚间 20 点到 22 点形成明显流量高峰。每小时购买比例使用 hourly_buy_rate 表示，即购买用户数与浏览用户数的比例。'),
(16, 0, 2, 'lb_time_hour_distribution(event_hour,pv,hourly_buy_rate)#mix'),
(16, 1, 3, '综合结论：#16 报告只保留逐小时分布，用于说明一天 0-23 点的流量变化和每小时购买比例。答辩时重点说明晚间流量最高，以及小时购买比例与 PV 不是同一个量级，所以图表只保留 PV 和比例两个核心指标。'),

(17, 1, 0, 'LuckyAnJun #17 热门类目 Top20 分析。本报告只保留 lb_category_topn 这一张结果表，横轴使用脱敏后的 category_id 表示具体类目。图表只展示 pv_cnt 和 category_conversion_rate，避免把 UV、收藏、加购、购买次数等多个指标混在一起导致报表阅读混乱。'),
(17, 1, 1, '一、热门类目 Top20。PV 最高的类目是 4756105，pv_cnt 为 278528，转化率 category_conversion_rate 为 5.78%；第二名类目 4145813 的 pv_cnt 为 184965，转化率为 7.77%；第三名类目 2355072 的 pv_cnt 为 181395，转化率为 2.77%。这说明热门类目的流量规模和购买效率并不完全一致。'),
(17, 0, 2, 'lb_category_topn(category_id,pv_cnt,category_conversion_rate)#mix'),
(17, 1, 3, '综合结论：#17 报告只保留热门类目 Top20，用于说明哪些脱敏类目获得最多浏览，以及这些类目的购买转化率是否同步较高。答辩时重点说明横轴是具体 category_id，指标只保留 PV 和转化率两个核心维度。'),

(18, 1, 0, 'LuckyAnJun #18 活跃天数分布分析。本报告只保留 lb_user_active_day_distribution 这一张结果表，横轴 active_days 按 1 到 9 递增展示，用于观察用户活跃天数、用户数占比、平均行为数和平均购买次数的变化。'),
(18, 1, 1, '二、活跃天数分布。横轴 active_days 已按 1-9 递增。活跃 9 天的用户最多，为 14570 人，占 26.09%；活跃 8 天用户为 11667 人，占 20.89%；活跃 7 天用户为 9853 人，占 17.64%。随着活跃天数增加，平均行为数和平均购买数整体上升，说明连续活跃用户贡献了更高的行为深度。'),
(18, 0, 2, 'lb_user_active_day_distribution(active_days,user_cnt,user_rate,avg_behavior_cnt,avg_buy_cnt)#mix'),
(18, 1, 3, '综合结论：#18 报告只保留活跃天数分布，用于说明用户在统计周期内活跃 1 到 9 天时的规模变化和行为深度变化。答辩时重点说明横轴已经递增排序，9 天、8 天和 7 天活跃用户最多，连续活跃天数越高，平均行为数和平均购买次数通常也更高。'),

(19, 1, 0, 'LuckyAnJun #19 实时 5 分钟窗口分析报告。本报告展示 Kafka + Spark Structured Streaming 的实时回放结果：历史 UserBehavior 日志被模拟推送到 Kafka topic，Spark 按 5 分钟事件时间窗口聚合 PV、近似 UV、收藏、加购、购买、热门类目、热门商品和异常预警，并写入 MySQL。该任务是历史日志实时回放模拟，不代表当前真实淘宝实时流量。'),
(19, 1, 1, '一、当前实时表状态。MySQL 中 lb_realtime_window_metrics 当前共有 30 个窗口，最新窗口为 2017-11-29 06:50 至 06:55。当前共享库里保存的是轻量回放或冒烟验证数据，窗口粒度较小，所有窗口的 alert_type 均为 low_traffic，说明实时链路已经写入窗口数据，但当前样本不足以做大规模业务波动判断。'),
(19, 0, 2, 'lb_realtime_window_metrics(window_start,pv,approx_uv,fav_cnt,cart_cnt,buy_cnt,buy_uv,pv_to_buy_rate)#mix'),
(19, 1, 3, '二、实时异常预警。当前 30 个窗口的 PV 合计为 17，购买行为合计为 9，平均浏览到购买用户转化率为 0。由于窗口内 PV 经常低于配置阈值，系统将其统一标记为 low_traffic。这一结果更适合作为实时任务链路验证：Kafka 消费、Spark 聚合、MySQL 更新和告警字段均能正常落表。'),
(19, 0, 4, 'lb_realtime_window_metrics(window_start,pv,buy_cnt,pv_to_buy_rate)#line'),
(19, 1, 5, '三、热门类目和热门商品。实时 Top 表按每个 5 分钟窗口内事件数排序。最新窗口中，类目 501 和商品 1015 各出现 1 次事件且均为购买行为。由于当前回放数据量很小，Top10 表主要用于验证实时排行榜逻辑，正式展示时应扩大回放量以形成更稳定的榜单。'),
(19, 0, 6, 'lb_realtime_category_top10(rank_no,event_cnt,buy_cnt)#bar'),
(19, 0, 7, 'lb_realtime_item_top10(rank_no,event_cnt,buy_cnt)#bar'),
(19, 1, 8, '综合结论：#19 报告可以证明实时分析链路已完成端到端闭环，但当前共享 MySQL 中的实时样本更偏测试性质。最终答辩展示时建议说明“历史日志实时回放模拟”的边界，并在现场或截图中展示窗口指标、Top 类目/商品和 low_traffic 告警字段。');

COMMIT;
