-- LuckyAnJun 离线分析3: 类目热度与商品转化效率分析
SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana;

DROP TABLE IF EXISTS lb_category_efficiency;
CREATE EXTERNAL TABLE lb_category_efficiency (
    category_id BIGINT,
    pv_cnt BIGINT,
    pv_uv BIGINT,
    fav_cnt BIGINT,
    cart_cnt BIGINT,
    buy_cnt BIGINT,
    buy_uv BIGINT,
    category_conversion_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana/lb_category_efficiency';

INSERT OVERWRITE TABLE lb_category_efficiency
SELECT
    category_id,
    COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END) AS pv_cnt,
    COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) AS pv_uv,
    COUNT(CASE WHEN behavior_type = 'fav' THEN 1 END) AS fav_cnt,
    COUNT(CASE WHEN behavior_type = 'cart' THEN 1 END) AS cart_cnt,
    COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt,
    COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS buy_uv,
    ROUND(
        CASE
            WHEN COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) = 0 THEN 0
            ELSE CAST(COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS DOUBLE)
                / COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END)
        END,
        4
    ) AS category_conversion_rate
FROM dwd_user_behavior_clean
WHERE user_id IS NOT NULL
  AND item_id IS NOT NULL
  AND category_id IS NOT NULL
  AND behavior_type IN ('pv', 'fav', 'cart', 'buy')
GROUP BY category_id
HAVING COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) > 0;

DROP TABLE IF EXISTS lb_item_efficiency;
CREATE EXTERNAL TABLE lb_item_efficiency (
    item_id BIGINT,
    category_id BIGINT,
    pv_cnt BIGINT,
    pv_uv BIGINT,
    fav_cnt BIGINT,
    cart_cnt BIGINT,
    intent_cnt BIGINT,
    buy_cnt BIGINT,
    buy_uv BIGINT,
    item_conversion_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana/lb_item_efficiency';

INSERT OVERWRITE TABLE lb_item_efficiency
SELECT
    item_id,
    category_id,
    COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END) AS pv_cnt,
    COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) AS pv_uv,
    COUNT(CASE WHEN behavior_type = 'fav' THEN 1 END) AS fav_cnt,
    COUNT(CASE WHEN behavior_type = 'cart' THEN 1 END) AS cart_cnt,
    COUNT(CASE WHEN behavior_type IN ('fav', 'cart') THEN 1 END) AS intent_cnt,
    COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt,
    COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS buy_uv,
    ROUND(
        CASE
            WHEN COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) = 0 THEN 0
            ELSE CAST(COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS DOUBLE)
                / COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END)
        END,
        4
    ) AS item_conversion_rate
FROM dwd_user_behavior_clean
WHERE user_id IS NOT NULL
  AND item_id IS NOT NULL
  AND category_id IS NOT NULL
  AND behavior_type IN ('pv', 'fav', 'cart', 'buy')
GROUP BY item_id, category_id
HAVING COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) > 0;

DROP TABLE IF EXISTS lb_category_topn;
CREATE EXTERNAL TABLE lb_category_topn (
    rank_no INT,
    category_id BIGINT,
    pv_cnt BIGINT,
    pv_uv BIGINT,
    fav_cnt BIGINT,
    cart_cnt BIGINT,
    buy_cnt BIGINT,
    buy_uv BIGINT,
    category_conversion_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana/lb_category_topn';

INSERT OVERWRITE TABLE lb_category_topn
SELECT
    rank_no,
    category_id,
    pv_cnt,
    pv_uv,
    fav_cnt,
    cart_cnt,
    buy_cnt,
    buy_uv,
    category_conversion_rate
FROM (
    SELECT
        ROW_NUMBER() OVER (ORDER BY pv_cnt DESC, buy_cnt DESC, category_id ASC) AS rank_no,
        category_id,
        pv_cnt,
        pv_uv,
        fav_cnt,
        cart_cnt,
        buy_cnt,
        buy_uv,
        category_conversion_rate
    FROM lb_category_efficiency
) ranked
WHERE rank_no <= 20;

DROP TABLE IF EXISTS lb_item_topn;
CREATE EXTERNAL TABLE lb_item_topn (
    rank_no INT,
    item_id BIGINT,
    category_id BIGINT,
    pv_cnt BIGINT,
    pv_uv BIGINT,
    intent_cnt BIGINT,
    buy_cnt BIGINT,
    buy_uv BIGINT,
    item_conversion_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana/lb_item_topn';

INSERT OVERWRITE TABLE lb_item_topn
SELECT
    rank_no,
    item_id,
    category_id,
    pv_cnt,
    pv_uv,
    intent_cnt,
    buy_cnt,
    buy_uv,
    item_conversion_rate
FROM (
    SELECT
        ROW_NUMBER() OVER (ORDER BY pv_cnt DESC, buy_cnt DESC, item_id ASC) AS rank_no,
        item_id,
        category_id,
        pv_cnt,
        pv_uv,
        intent_cnt,
        buy_cnt,
        buy_uv,
        item_conversion_rate
    FROM lb_item_efficiency
) ranked
WHERE rank_no <= 20;

DROP TABLE IF EXISTS lb_category_conversion_rank;
CREATE EXTERNAL TABLE lb_category_conversion_rank (
    rank_no INT,
    category_id BIGINT,
    pv_cnt BIGINT,
    pv_uv BIGINT,
    fav_cnt BIGINT,
    cart_cnt BIGINT,
    buy_cnt BIGINT,
    buy_uv BIGINT,
    category_conversion_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana/lb_category_conversion_rank';

INSERT OVERWRITE TABLE lb_category_conversion_rank
SELECT
    rank_no,
    category_id,
    pv_cnt,
    pv_uv,
    fav_cnt,
    cart_cnt,
    buy_cnt,
    buy_uv,
    category_conversion_rate
FROM (
    SELECT
        ROW_NUMBER() OVER (ORDER BY category_conversion_rate DESC, buy_uv DESC, pv_cnt DESC) AS rank_no,
        category_id,
        pv_cnt,
        pv_uv,
        fav_cnt,
        cart_cnt,
        buy_cnt,
        buy_uv,
        category_conversion_rate
    FROM lb_category_efficiency
    WHERE pv_uv >= 100
) ranked
WHERE rank_no <= 20;

DROP TABLE IF EXISTS lb_category_low_conversion;
CREATE EXTERNAL TABLE lb_category_low_conversion (
    rank_no INT,
    category_id BIGINT,
    pv_cnt BIGINT,
    pv_uv BIGINT,
    fav_cnt BIGINT,
    cart_cnt BIGINT,
    buy_cnt BIGINT,
    buy_uv BIGINT,
    category_conversion_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana/lb_category_low_conversion';

INSERT OVERWRITE TABLE lb_category_low_conversion
SELECT
    rank_no,
    category_id,
    pv_cnt,
    pv_uv,
    fav_cnt,
    cart_cnt,
    buy_cnt,
    buy_uv,
    category_conversion_rate
FROM (
    SELECT
        ROW_NUMBER() OVER (ORDER BY category_conversion_rate ASC, pv_cnt DESC) AS rank_no,
        category_id,
        pv_cnt,
        pv_uv,
        fav_cnt,
        cart_cnt,
        buy_cnt,
        buy_uv,
        category_conversion_rate
    FROM lb_category_efficiency
    WHERE pv_cnt >= (SELECT AVG(pv_cnt) FROM lb_category_efficiency)
) ranked
WHERE rank_no <= 20;

DROP TABLE IF EXISTS lb_item_long_tail;
CREATE EXTERNAL TABLE lb_item_long_tail (
    rank_no INT,
    item_id BIGINT,
    category_id BIGINT,
    pv_cnt BIGINT,
    pv_uv BIGINT,
    buy_cnt BIGINT,
    buy_uv BIGINT,
    item_conversion_rate DOUBLE,
    cumulative_pv BIGINT,
    total_pv BIGINT,
    cumulative_pv_rate DOUBLE,
    tail_segment STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/bigdata_ana/lb_item_long_tail';

INSERT OVERWRITE TABLE lb_item_long_tail
SELECT
    rank_no,
    item_id,
    category_id,
    pv_cnt,
    pv_uv,
    buy_cnt,
    buy_uv,
    item_conversion_rate,
    cumulative_pv,
    total_pv,
    cumulative_pv_rate,
    CASE
        WHEN cumulative_pv_rate <= 0.8 THEN 'head'
        WHEN cumulative_pv_rate <= 0.95 THEN 'middle'
        ELSE 'long_tail'
    END AS tail_segment
FROM (
    SELECT
        ROW_NUMBER() OVER (ORDER BY pv_cnt DESC, buy_cnt DESC, item_id ASC) AS rank_no,
        item_id,
        category_id,
        pv_cnt,
        pv_uv,
        buy_cnt,
        buy_uv,
        item_conversion_rate,
        SUM(pv_cnt) OVER (ORDER BY pv_cnt DESC, buy_cnt DESC, item_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pv,
        SUM(pv_cnt) OVER () AS total_pv,
        ROUND(
            CAST(SUM(pv_cnt) OVER (ORDER BY pv_cnt DESC, buy_cnt DESC, item_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DOUBLE)
                / SUM(pv_cnt) OVER (),
            4
        ) AS cumulative_pv_rate
    FROM lb_item_efficiency
) ranked;

SELECT * FROM lb_category_topn ORDER BY rank_no;
SELECT * FROM lb_item_topn ORDER BY rank_no;
SELECT * FROM lb_category_conversion_rank ORDER BY rank_no;
SELECT * FROM lb_category_low_conversion ORDER BY rank_no;
SELECT * FROM lb_item_long_tail ORDER BY rank_no LIMIT 20;
