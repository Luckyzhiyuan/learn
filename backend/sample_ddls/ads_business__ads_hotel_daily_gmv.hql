CREATE TABLE ads_business.ads_hotel_daily_gmv (
    report_date   STRING COMMENT '报表日期',
    hotel_id      BIGINT COMMENT '酒店ID',
    city_id       STRING COMMENT '城市ID',
    total_gmv     BIGINT COMMENT '当日GMV(分)',
    order_cnt     BIGINT COMMENT '订单数',
    pv            BIGINT COMMENT '页面浏览量',
    uv            BIGINT COMMENT '独立访客数'
)
COMMENT '酒店每日GMV指标汇总'
PARTITIONED BY (report_date STRING)
STORED AS ORC
TBLPROPERTIES ('compression'='SNAPPY');