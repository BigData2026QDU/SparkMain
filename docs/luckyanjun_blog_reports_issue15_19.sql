SET NAMES utf8mb4;

DROP TABLE IF EXISTS lb_funnel_item_path_stage;
CREATE TABLE lb_funnel_item_path_stage (
  stage_order INT NOT NULL,
  stage_name VARCHAR(64) NOT NULL,
  pair_cnt BIGINT NOT NULL,
  conversion_rate DOUBLE DEFAULT NULL,
  PRIMARY KEY (stage_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO lb_funnel_item_path_stage (stage_order, stage_name, pair_cnt, conversion_rate) VALUES
(1, '浏览商品', 6085537, 1.0),
(2, '浏览后收藏/加购同商品', 97398, 0.016005),
(3, '意向后购买同商品', 8835, 0.001452);

START TRANSACTION;

DELETE FROM blog WHERE bindex IN (15, 16, 17, 18, 19);

INSERT INTO blog (bindex, btype, paragraph, content, is_realtime) VALUES
(15, 1, 0, 'LuckyAnJun #15 严格同商品路径漏斗。本报告不再使用“用户在周期内是否到达过某类行为”的宽口径，而是改用用户-商品对和时间顺序：先浏览同一商品，再对同一商品收藏或加购，最后购买同一商品。该口径基于 300MB 截断 UserBehavior 数据 8571842 行，更适合说明真实行为路径。', 0),
(15, 1, 1, '严格路径规模。浏览过商品的用户-商品对为 6085537 个；浏览后对同一商品产生收藏或加购的用户-商品对为 97398 个；产生意向后继续购买同一商品的用户-商品对为 8835 个。浏览到意向转化率为 1.60%，浏览后经意向购买的整体转化率为 0.15%，比原来的用户阶段到达漏斗更严格，也更符合电商行为路径。', 0),
(15, 0, 2, 'lb_funnel_item_path_stage(stage_name,pair_cnt)#bar', 0),
(15, 1, 3, '综合结论：#15 报告现在只保留这一张严格路径漏斗表，图中只展示三个阶段的用户-商品对数量，转化率放在文字中说明。这样页面重点更集中：大多数浏览没有进入收藏或加购，能进入意向阶段的同商品行为很少，而最终完成意向后购买的规模进一步收窄。答辩时应强调：原阶段到达漏斗用于总览，这个同商品路径漏斗更适合解释真实转化过程。', 0),

(16, 1, 0, 'LuckyAnJun #16 逐小时流量与购买比例分析。本报告只保留 lb_time_hour_distribution 这一张逐小时结果表，横轴按 0 点到 23 点顺序展示。图表只展示 PV 和 hourly_buy_rate，避免多个指标混在一起导致报表阅读混乱。', 0),
(16, 1, 1, '逐小时概况。按 24 小时聚合后，21 点 PV 最高，为 431141；22 点为 422791；20 点为 376042，晚间 20 点到 22 点形成明显流量高峰。每小时购买比例使用 hourly_buy_rate 表示，即购买用户数与浏览用户数的比例。', 0),
(16, 0, 2, 'lb_time_hour_distribution(event_hour,pv,hourly_buy_rate)#mix', 0),
(16, 1, 3, '综合结论：#16 报告只保留逐小时分布，用于说明一天 0-23 点的流量变化和每小时购买比例。答辩时重点说明晚间流量最高，以及小时购买比例与 PV 不是同一个量级，所以图表只保留 PV 和比例两个核心指标。', 0),

(17, 1, 0, 'LuckyAnJun #17 热门类目 Top20 分析。本报告只保留 lb_category_topn 这一张结果表，横轴使用脱敏后的 category_id 表示具体类目。图表只展示 pv_cnt 和 category_conversion_rate，避免把 UV、收藏、加购、购买次数等多个指标混在一起导致报表阅读混乱。', 0),
(17, 1, 1, '热门类目 Top20。PV 最高的类目是 4756105，pv_cnt 为 278528，转化率 category_conversion_rate 为 5.78%；第二名类目 4145813 的 pv_cnt 为 184965，转化率为 7.77%；第三名类目 2355072 的 pv_cnt 为 181395，转化率为 2.77%。这说明热门类目的流量规模和购买效率并不完全一致。', 0),
(17, 0, 2, 'lb_category_topn(category_id,pv_cnt,category_conversion_rate)#mix', 0),
(17, 1, 3, '综合结论：#17 报告只保留热门类目 Top20，用于说明哪些脱敏类目获得最多浏览，以及这些类目的购买转化率是否同步较高。答辩时重点说明横轴是具体 category_id，指标只保留 PV 和转化率两个核心维度。', 0),

(18, 1, 0, 'LuckyAnJun #18 活跃天数分布分析。本报告只保留 lb_user_active_day_distribution 这一张结果表，横轴 active_days 按 1 到 9 递增展示，用于观察用户活跃天数、用户数占比、平均行为数和平均购买次数的变化。', 0),
(18, 1, 1, '活跃天数分布。横轴 active_days 已按 1-9 递增。活跃 9 天的用户最多，为 14570 人，占 26.09%；活跃 8 天用户为 11667 人，占 20.89%；活跃 7 天用户为 9853 人，占 17.64%。随着活跃天数增加，平均行为数和平均购买数整体上升，说明连续活跃用户贡献了更高的行为深度。', 0),
(18, 0, 2, 'lb_user_active_day_distribution(active_days,user_cnt,user_rate,avg_behavior_cnt,avg_buy_cnt)#mix', 0),
(18, 1, 3, '综合结论：#18 报告只保留活跃天数分布，用于说明用户在统计周期内活跃 1 到 9 天时的规模变化和行为深度变化。答辩时重点说明横轴已经递增排序，9 天、8 天和 7 天活跃用户最多，连续活跃天数越高，平均行为数和平均购买次数通常也更高。', 0),

(19, 1, 0, 'LuckyAnJun #19 Blog 实时 5 分钟窗口监控。历史 UserBehavior 日志逐条回放到 Kafka，Spark Structured Streaming 按事件时间持续计算最近窗口的 PV、收藏、加购、购买和异常等级，并写入 MySQL。Blog 图表块每 1 秒重新查询，无需单独实时页面。', 0),
(19, 1, 1, '实时指标说明：PV 反映浏览流量，收藏表示兴趣，加购表示较强购买意向，购买表示最终行为次数；alert_level 将预警直观量化为 0=正常、1=低流量或低转化提醒、2=流量突增或骤降。图中只保留最近 12 个窗口，新窗口会从右侧持续出现。', 0),
(19, 0, 2, 'lb_realtime_blog_metrics(window_label,pv,fav_cnt,cart_cnt,buy_cnt,alert_level)#mix', 1),
(19, 1, 3, '综合结论：Kafka 输入、Spark micro-batch 聚合、MySQL 最近窗口表和 Blog 每秒刷新构成实时闭环。答辩时启动回放后，可直接在报告 #19 看到窗口推进、行为指标变化和预警等级跳变；这是历史日志实时回放模拟，不代表淘宝官方实时流量。', 0);

COMMIT;
