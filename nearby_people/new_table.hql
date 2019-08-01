CREATE EXTERNAL TABLE IF NOT EXISTS recommend_nearby_test.recommend_nearby_people_phone_price
(
  brand STRING COMMENT '手机品牌'
  ,phone STRING COMMENT '手机名'
  ,price float COMMENT '价格'
  ,price_range STRING COMMENT '价格范围'
  ,price_tag bigint COMMENT '价格标记'
)
PARTITIONED BY (partition_version STRING COMMENT '版本')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' 
COLLECTION ITEMS TERMINATED BY '\t'
MAP KEYS TERMINATED BY '\t'
STORED AS TEXTFILE;



CREATE EXTERNAL TABLE IF NOT EXISTS recommend_nearby.tl_nearby_people_onecase_raw
(
  log_id STRING COMMENT '推荐请求的唯一id'
  ,rec_time bigint COMMENT '请求时间'
  ,from_momo_id STRING COMMENT 'from_momo_id'
  ,to_momo_id STRING COMMENT 'to_momo_id'
  ,from_realtime_feature STRING COMMENT 'from侧特征'
  ,to_realtime_feature STRING COMMENT 'to侧特征'
  ,is_show bigint COMMENT '是否曝光'
  ,is_click bigint COMMENT '是否点击'
  ,is_sayHi bigint COMMENT '是否打招呼'
  ,is_reply bigint COMMENT '打招呼是否回复'
  ,show_time bigint COMMENT '曝光时间'
  ,click_time bigint COMMENT '点击时间'
  ,sayhi_time bigint COMMENT '第一次打招呼时间'
  ,reply_time bigint COMMENT '第一次回复时间'
  ,msg_round bigint COMMENT '聊天轮数（24小时）'
  ,user_agent STRING COMMENT '用户UA信息'
)
PARTITIONED BY (partition_date STRING COMMENT '分区日期')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001' 
COLLECTION ITEMS TERMINATED BY '\002'
MAP KEYS TERMINATED BY '\003'
STORED AS TEXTFILE;