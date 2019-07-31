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