SET NAMES utf8mb4;

START TRANSACTION;

DELETE FROM blog WHERE bindex IN (15, 16, 17, 18, 19);

INSERT INTO blog (bindex, btype, paragraph, content) VALUES
(15, 1, 0, 'LuckyAnJun #15 用户行为整体漏斗概况。本报告基于淘宝 UserBehavior 清洗后的明细表，由 Spark 离线任务计算并导出到 MySQL。分析口径为：pv 表示浏览，fav 和 cart 合并为意向行为，buy 表示购买行为；该漏斗用于观察用户是否曾到达某个行为阶段，不假设严格的 pv -> fav/cart -> buy 顺序。'),
(15, 1, 1, '一、整体漏斗概况。当前结果中，发生过浏览的用户为 55611 人，发生过收藏或加购意向的用户为 48831 人，发生过购买的用户为 38019 人。浏览用户中同时发生意向行为的用户为 48605 人，转化率为 87.40%；浏览用户中最终购买的用户为 37836 人，转化率为 68.04%。这说明在当前采样数据中，大部分浏览用户并非只停留在浏览阶段，而是继续产生了明确兴趣或购买行为。'),
(15, 0, 2, 'lb_funnel_overall(pv_users,intent_users,buy_users,pv_to_intent_users,intent_to_buy_users,pv_to_buy_users)#bar'),
(15, 1, 3, '综合结论：#15 报告只保留整体漏斗概况，用于展示浏览、意向和购买三个阶段的总体用户规模与总体转化关系。该报表不展开每日趋势和流失拆分，答辩时重点说明整体漏斗口径和核心转化率。'),

(16, 1, 0, 'LuckyAnJun #16 时段流量与购买高峰分析。本报告使用 lb_time_hour_distribution、lb_time_high_conversion_slots、lb_time_low_conversion_slots 和 lb_time_weekday_hour_heatmap 等结果表，分析 24 小时流量分布、购买高峰和低转化时段。'),
(16, 1, 1, '一、小时流量高峰。按 24 小时聚合后，21 点 PV 最高，为 431141；22 点为 422791；20 点为 376042。20 点到 22 点形成明显晚间流量高峰，并且对应购买行为也较活跃，说明用户在晚间集中浏览和决策，适合作为活动曝光、推荐排序和运营推送的重点时段。'),
(16, 0, 2, 'lb_time_hour_distribution(event_hour,pv,pv_uv,buy_cnt,buy_uv,hourly_buy_rate)#mix'),
(16, 1, 3, '二、高转化时段。按日期和小时粒度筛选后，2017-11-27 0 点的小时购买转化率最高，为 15.03%；2017-12-01 10 点为 12.04%；2017-11-30 10 点为 11.77%。这些时段的 PV 规模不是全局最高，但购买用户占浏览用户比例更高，说明高转化时段与高流量时段并不完全重合。'),
(16, 0, 4, 'lb_time_high_conversion_slots(event_hour,pv_uv,buy_uv,hourly_buy_rate)#mix'),
(16, 1, 5, '三、低转化时段。低转化 Top 时段集中在周末早晨或晚间低效流量中，例如 2017-12-02 8 点转化率为 6.82%，2017-12-03 8 点为 6.93%，2017-12-03 0 点为 7.58%。这些时段虽然仍有一定 PV，但购买承接效率较弱，适合在报告中作为流量质量分析的反例。'),
(16, 0, 6, 'lb_time_low_conversion_slots(event_hour,pv_uv,buy_uv,hourly_buy_rate)#mix'),
(16, 1, 7, '四、星期与小时组合。lb_time_weekday_hour_heatmap 将星期和小时组合成 168 个观察点，可用于发现同一小时在不同星期下的差异。该表适合做热力图或折线对比；当前网站图表协议没有专门 heatmap 类型，因此用折线/混合图展示核心指标。'),
(16, 0, 8, 'lb_time_weekday_hour_heatmap(weekday,pv,buy_uv,buy_rate)#mix'),
(16, 1, 9, '综合结论：#16 报告的主要价值是区分“流量高峰”和“购买效率高峰”。晚间 20-22 点贡献最大流量，而部分上午和凌晨时段在转化率上更突出。最终展示时建议优先保留小时分布表和高低转化时段表。'),

(17, 1, 0, 'LuckyAnJun #17 类目热度与商品转化效率分析。本报告围绕 lb_category_topn、lb_item_topn、lb_category_conversion_rank 和 lb_category_low_conversion，分析热门类目、热门商品、高转化类目和低转化类目。由于 UserBehavior 数据集中没有价格、订单金额和商品名称，本报告只分析行为热度和行为转化，不做 GMV 或销售额判断。'),
(17, 1, 1, '一、热门类目 Top20。PV 最高的类目是 4756105，PV 为 278528，PV 用户数为 24335，购买用户数为 1406，转化率为 5.78%；第二名类目 4145813 的 PV 为 184965，购买用户数为 1684，转化率为 7.77%。这说明类目热度和购买效率存在差异，不能只按 PV 判断业务价值。'),
(17, 0, 2, 'lb_category_topn(rank_no,pv_cnt,pv_uv,buy_cnt,buy_uv,category_conversion_rate)#mix'),
(17, 1, 3, '二、热门商品 Top20。商品 812879 的 PV 最高，为 1703，但购买用户数只有 7，转化率为 0.59%；排名第 6 的商品 3031354 PV 为 1060，购买用户数为 38，转化率为 4.94%。这说明头部曝光商品中也存在“高浏览低转化”和“曝光较少但购买效率较好”的差异。'),
(17, 0, 4, 'lb_item_topn(rank_no,pv_cnt,intent_cnt,buy_cnt,buy_uv,item_conversion_rate)#mix'),
(17, 1, 5, '三、高转化类目。满足最小浏览用户条件后，类目 804084 的购买转化率最高，为 68.57%；类目 4392650 为 38.89%；类目 63690 为 37.72%。这些类目的 PV 规模并不一定最大，但购买用户占比明显更高，适合在运营分析中作为高效率类目单独观察。'),
(17, 0, 6, 'lb_category_conversion_rank(rank_no,pv_uv,buy_uv,category_conversion_rate)#mix'),
(17, 1, 7, '四、低转化类目。低转化表中，类目 3795317 的 PV 为 1031，但购买用户数为 0，转化率为 0；类目 4825728 的 PV 为 1382，购买用户数为 1，转化率仅 0.16%。这类结果可以帮助识别“有流量但缺少购买”的类目，后续展示时适合与热门类目表一起对比。'),
(17, 0, 8, 'lb_category_low_conversion(rank_no,pv_cnt,buy_cnt,category_conversion_rate)#mix'),
(17, 1, 9, '综合结论：#17 报告适合体现“热度”和“效率”两个不同维度。热门类目、热门商品用于说明用户注意力集中，高转化和低转化类目用于说明同样有流量时购买效率差异明显。最终展示建议至少保留 lb_category_topn、lb_item_topn 和 lb_category_conversion_rank。'),

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
