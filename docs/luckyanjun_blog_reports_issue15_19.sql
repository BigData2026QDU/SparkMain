SET NAMES utf8mb4;

START TRANSACTION;

DELETE FROM blog WHERE bindex IN (15, 16, 17, 18, 19);

INSERT INTO blog (bindex, btype, paragraph, content) VALUES
(15, 1, 0, 'LuckyAnJun #15 用户行为整体漏斗概况。本报告基于淘宝 UserBehavior 清洗后的明细表，由 Spark 离线任务计算并导出到 MySQL。分析口径为：pv 表示浏览，fav 和 cart 合并为意向行为，buy 表示购买行为；该漏斗用于观察用户是否曾到达某个行为阶段，不假设严格的 pv -> fav/cart -> buy 顺序。'),
(15, 1, 1, '一、整体漏斗概况。当前结果中，发生过浏览的用户为 55611 人，发生过收藏或加购意向的用户为 48831 人，发生过购买的用户为 38019 人。浏览用户中同时发生意向行为的用户为 48605 人，转化率为 87.40%；浏览用户中最终购买的用户为 37836 人，转化率为 68.04%。这说明在当前采样数据中，大部分浏览用户并非只停留在浏览阶段，而是继续产生了明确兴趣或购买行为。'),
(15, 0, 2, 'lb_funnel_overall(pv_users,intent_users,buy_users,pv_to_intent_users,intent_to_buy_users,pv_to_buy_users)#bar'),
(15, 1, 3, '综合结论：#15 报告只保留整体漏斗概况，用于展示浏览、意向和购买三个阶段的总体用户规模与总体转化关系。该报表不展开每日趋势和流失拆分，答辩时重点说明整体漏斗口径和核心转化率。'),

(16, 1, 0, 'LuckyAnJun #16 逐小时流量与购买比例分析。本报告只保留 lb_time_hour_distribution 这一张逐小时结果表，横轴按 0 点到 23 点顺序展示。图表只展示 PV 和 hourly_buy_rate，避免多个指标混在一起导致报表阅读混乱。'),
(16, 1, 1, '一、逐小时概况。按 24 小时聚合后，21 点 PV 最高，为 431141；22 点为 422791；20 点为 376042，晚间 20 点到 22 点形成明显流量高峰。每小时购买比例使用 hourly_buy_rate 表示，即购买用户数与浏览用户数的比例。'),
(16, 0, 2, 'lb_time_hour_distribution(event_hour,pv,hourly_buy_rate)#mix'),
(16, 1, 3, '综合结论：#16 报告只保留逐小时分布，用于说明一天 0-23 点的流量变化和每小时购买比例。答辩时重点说明晚间流量最高，以及小时购买比例与 PV 不是同一个量级，所以图表只保留 PV 和比例两个核心指标。'),

(17, 1, 0, 'LuckyAnJun #17 热门类目 Top20 分析。本报告只保留 lb_category_topn 这一张结果表，横轴使用脱敏后的 category_id 表示具体类目。图表只展示 pv_cnt 和 category_conversion_rate，避免把 UV、收藏、加购、购买次数等多个指标混在一起导致报表阅读混乱。'),
(17, 1, 1, '一、热门类目 Top20。PV 最高的类目是 4756105，pv_cnt 为 278528，转化率 category_conversion_rate 为 5.78%；第二名类目 4145813 的 pv_cnt 为 184965，转化率为 7.77%；第三名类目 2355072 的 pv_cnt 为 181395，转化率为 2.77%。这说明热门类目的流量规模和购买效率并不完全一致。'),
(17, 0, 2, 'lb_category_topn(category_id,pv_cnt,category_conversion_rate)#mix'),
(17, 1, 3, '综合结论：#17 报告只保留热门类目 Top20，用于说明哪些脱敏类目获得最多浏览，以及这些类目的购买转化率是否同步较高。答辩时重点说明横轴是具体 category_id，指标只保留 PV 和转化率两个核心维度。'),

(18, 1, 0, 'LuckyAnJun #18 用户分层、留存与短期复购分析。本报告基于用户粒度特征表计算生命周期分层、活跃天数分布、留存和短期复购差异。用户分层只使用行为数据，不包含性别、年龄、城市等画像字段。'),
(18, 1, 1, '一、生命周期分层。短期复购用户为 20892 人，占 37.41%；首购用户为 17127 人，占 30.67%；只有收藏或加购但未购买的意向用户为 14558 人，占 26.07%；只浏览用户为 3264 人，占 5.85%。另外，高活跃用户为 6810 人，占 12.20%，平均行为数达到 254.59，远高于普通用户。'),
(18, 0, 2, 'lb_user_segment_summary(user_segment,user_cnt,user_rate,avg_active_days,avg_behavior_cnt,avg_buy_cnt)#mix'),
(18, 1, 3, '二、活跃天数分布。活跃 9 天的用户最多，为 14570 人，占 26.09%；活跃 8 天用户为 11667 人，占 20.89%；活跃 7 天用户为 9853 人，占 17.64%。随着活跃天数增加，平均行为数和平均购买数也同步上升，说明连续活跃用户贡献了更高的行为深度。'),
(18, 0, 4, 'lb_user_active_day_distribution(active_days,user_cnt,user_rate,avg_behavior_cnt,avg_buy_cnt)#mix'),
(18, 1, 5, '三、留存表现。2017-11-25 cohort 用户数为 39850，次日留存率 78.80%，3 日留存率 76.37%，7 日留存率 98.54%。需要注意，后续 cohort 因观察窗口不足，7 日留存会出现 0，不能解释为真实业务留存断崖，而是由数据时间范围限制导致。'),
(18, 0, 6, 'lb_user_retention(cohort_date,cohort_users,day1_retention_rate,day3_retention_rate,day7_retention_rate)#mix'),
(18, 1, 7, '四、复购行为深度。短期复购用户平均活跃 7.64 天，平均行为数 127.07，平均购买次数 4.32；非复购用户平均活跃 6.72 天，平均行为数 87.36，平均购买次数 0.69。复购用户不仅购买次数更多，浏览、收藏和加购等前置行为也更深。'),
(18, 0, 8, 'lb_repurchase_behavior_depth(repurchase_group,user_cnt,avg_active_days,avg_behavior_cnt,avg_intent_cnt,avg_buy_cnt,avg_buy_days)#mix'),
(18, 1, 9, '综合结论：#18 报告适合用于展示用户价值分层。短期复购用户和高活跃用户是报告中最有解释力的群体；留存表可展示 cohort 思路，但需要明确 2017-11-25 至 2017-12-03 的观察窗口限制。'),

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
