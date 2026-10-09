CREATE TABLE mid_hotel.dwd_hotel_room_inventory (
    hotel_id      BIGINT COMMENT '酒店ID',
    room_type_id  BIGINT COMMENT '房型ID',
    inventory     BIGINT COMMENT '库存量',
    dt            STRING COMMENT '分区日期'
)
COMMENT '酒店房间库存(事实)'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('compression'='SNAPPY');