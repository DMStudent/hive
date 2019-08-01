-- onecase 数据与手机价格信息join，获取每个用户的手机价格
-- 分析不用价位手机用户的点击率等行为差异

SELECT
	phone_tab.price_tag
    ,sum(onecase_tab.is_show) as num_show
    ,sum(onecase_tab.is_click) as num_click
    ,sum(onecase_tab.is_sayhi) as num_sayhi
    ,sum(onecase_tab.is_reply) as num_reply
    ,sum(onecase_tab.is_click)/sum(onecase_tab.is_show) as click_ratio  
    ,sum(onecase_tab.is_sayhi)/sum(onecase_tab.is_show) as sayhi_ratio
    ,sum(onecase_tab.is_reply)/sum(onecase_tab.is_show) as reply_ratio
FROM
(
    SELECT
        is_show	
        ,is_click	
        ,is_sayhi	
        ,is_reply
        ,regexp_replace(trim(split(trim(split(user_agent,'\\(')[1]),';')[size(split(trim(split(user_agent,'\\(')[1]),';'))-1]),'\\)','')  as brand
        ,trim(split(trim(split(user_agent,'\\(')[1]),';')[0]) as phone
    FROM recommend_nearby.tl_nearby_people_onecase_raw
    WHERE partition_date=20190730
  
) onecase_tab
JOIN                           
(  
    SELECT
  		phone
  		,price_tag
    FROM recommend_nearby_test.recommend_nearby_people_phone_price
    WHERE partition_version=1 
) phone_tab
ON onecase_tab.phone=phone_tab.phone
GROUP by phone_tab.price_tag                     








-- 根据onecase统计点击率、招呼率、回复率
SELECT
    sum(onecase_tab.is_show) as num_show
    ,sum(onecase_tab.is_click) as num_click
    ,sum(onecase_tab.is_sayhi) as num_sayhi
    ,sum(onecase_tab.is_reply) as num_reply
    ,sum(onecase_tab.is_click)/sum(onecase_tab.is_show) as click_ratio  
    ,sum(onecase_tab.is_sayhi)/sum(onecase_tab.is_show) as sayhi_ratio
    ,sum(onecase_tab.is_reply)/sum(onecase_tab.is_show) as reply_ratio
FROM
(
    SELECT
        is_show	
        ,is_click	
        ,is_sayhi	
        ,is_reply
    FROM recommend_nearby.tl_nearby_people_onecase_raw
    WHERE partition_date=20190730
  
) onecase_tab
                
