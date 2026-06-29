-- LuckyAnJun 离线分析 #17：热门类目 Top20。
SET hive.execution.engine=spark;
SET spark.master=local[*];

USE bigdata_ana;

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
STORED AS PARQUET
LOCATION '/user/hive/bigdata_ana/lb_category_topn';

INSERT OVERWRITE TABLE lb_category_topn
SELECT
    CAST(rank_no AS INT),
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
        ROW_NUMBER() OVER (
            ORDER BY pv_cnt DESC, buy_cnt DESC, category_id ASC
        ) AS rank_no,
        category_id,
        pv_cnt,
        pv_uv,
        fav_cnt,
        cart_cnt,
        buy_cnt,
        buy_uv,
        ROUND(CAST(buy_uv AS DOUBLE) / pv_uv, 4) AS category_conversion_rate
    FROM (
        SELECT
            category_id,
            COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END) AS pv_cnt,
            COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) AS pv_uv,
            COUNT(CASE WHEN behavior_type = 'fav' THEN 1 END) AS fav_cnt,
            COUNT(CASE WHEN behavior_type = 'cart' THEN 1 END) AS cart_cnt,
            COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt,
            COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN user_id END) AS buy_uv
        FROM dwd_user_behavior_clean
        WHERE user_id IS NOT NULL
          AND category_id IS NOT NULL
          AND behavior_type IN ('pv', 'fav', 'cart', 'buy')
        GROUP BY category_id
        HAVING COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN user_id END) > 0
    ) category_stats
) ranked
WHERE rank_no <= 20;

SELECT * FROM lb_category_topn ORDER BY rank_no;
