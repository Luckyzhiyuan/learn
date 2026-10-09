CREATE TABLE mid_hotel.dwd_hotel_order_detail (
    order_id       BIGINT COMMENT '订单ID',
    hotel_id       BIGINT COMMENT '酒店ID',
    userkey        BIGINT COMMENT '用户唯一键',
    city_id        STRING COMMENT '城市ID',
    gmv            BIGINT COMMENT '订单金额(分)',
    room_night     BIGINT COMMENT '间夜数',
    order_status   STRING COMMENT '订单状态',
    dt             STRING COMMENT '分区日期'
)
COMMENT '酒店订单明细(事实)'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('compression'='SNAPPY');