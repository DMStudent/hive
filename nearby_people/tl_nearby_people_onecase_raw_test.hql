ADD JAR hdfs://nameservice1/user/hive/warehouse/bin/udfs/momo-hive-latest.jar;
CREATE TEMPORARY FUNCTION from_json AS 'com.immomo.hive.common.udf.UDFFromJson';
CREATE TEMPORARY FUNCTION to_json AS 'com.immomo.hive.common.udf.UDFToJson';

ADD JAR hdfs://nameservice1/user/recommend/udfs/recommend-udfs-1.0.3-SNAPSHOT-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION getExperimentAssignment AS 'com.immomo.recommend.udfs.GetExperiment';

insert overwrite table recommend_nearby.tl_nearby_people_onecase_raw PARTITION (partition_date='${hivevar:partition_yesterday}') 

select 
  	show_click_tab.log_id as log_id
    ,show_click_tab.rec_time as rec_time
    ,show_click_tab.from_momo_id as from_momo_id
    ,show_click_tab.to_momo_id as to_momo_id
    -- ,COALESCE(hi_tab.has_send_first_sayhi, 0) as is_sayHi
    -- ,COALESCE(hi_tab.is_replyed_sayhi_today, 0) as is_reply
    -- ,show_click_tab.from_gender as from_gender
    -- ,show_click_tab.to_gender as to_gender
    -- ,show_click_tab.request_params as request_params
    ,show_click_tab.from_realtime_feature as from_realtime_feature
    ,show_click_tab.to_realtime_feature as to_realtime_feature
    -- ,show_click_tab.num_chat as num_chat
    -- ,show_click_tab.last_chat_time as last_chat_time
    -- ,show_click_tab.hour as hour
    ,show_click_tab.is_show as is_show
    ,show_click_tab.is_click
    ,CASE WHEN hi_tab.sayhi_timestamp IS NOT NULL AND hi_tab.sayhi_timestamp>0 then 1 else 0 END AS is_sayHi
    ,CASE WHEN hi_tab.reply_timestamp IS NOT NULL AND hi_tab.reply_timestamp>0 then 1 else 0 END AS is_reply    
    ,show_click_tab.show_time as show_time
    ,show_click_tab.click_time as click_time
    ,CASE WHEN hi_tab.sayhi_timestamp IS NOT NULL then hi_tab.sayhi_timestamp else 0 END AS sayhi_time
    ,CASE WHEN hi_tab.reply_timestamp IS NOT NULL then hi_tab.reply_timestamp else 0 END AS reply_time 
    ,0 AS msg_round
    ,show_click_tab.user_agent as user_agent
from
(
    select 
        show_tab.log_id as log_id
        ,show_tab.rec_time as rec_time
        ,show_tab.from_momo_id as from_momo_id
        ,show_tab.to_momo_id as to_momo_id
        ,show_tab.is_show as is_show
        ,CASE WHEN click_tab.from_momo_id IS NOT NULL AND click_tab.to_momo_id IS NOT NULL AND click_tab.log_id IS NOT NULL then 1 else 0 END AS is_click
        ,show_tab.from_realtime_feature as from_realtime_feature
        ,show_tab.to_realtime_feature as to_realtime_feature
        ,show_tab.show_time as show_time
        ,CASE WHEN click_tab.click_time IS NOT NULL then click_tab.click_time else 0 END AS click_time
        ,show_tab.user_agent as user_agent
    from    
    (
    select 
        exposure_log.log_id as log_id
        ,exposure_log.rec_time as rec_time
        ,exposure_log.from_momo_id as from_momo_id
        ,exposure_log.to_momo_id as to_momo_id
        ,CASE WHEN event_show.from_momo_id IS NOT NULL AND event_show.to_momo_id IS NOT NULL AND event_show.log_id IS NOT NULL then 1 else 0 END AS is_show
        ,exposure_log.from_realtime_feature as from_realtime_feature
        ,exposure_log.to_realtime_feature as to_realtime_feature
        ,event_show.display_timestamp as show_time
        ,event_show.user_agent as user_agent
    from
    (
        select
            log_id
            ,rec_time
            ,from_momo_id
            ,to_momo_id
            ,exposure_item
            ,request_params as from_realtime_feature
            ,exposure_item as to_realtime_feature
        from recommend_nearby.bl_recommend_online_exposure_data
        where partition_date = '${hivevar:partition_yesterday}'   
    ) exposure_log 
    join
    (
        SELECT
            from_momo_id
            ,to_momo_id
            ,log_id
            ,display_timestamp
            ,user_agent
        FROM 
        (
            SELECT 
                momo_id AS from_momo_id
                ,CAST(timestamp AS bigint) AS display_timestamp
                ,CASE 
                    WHEN TRIM(SPLIT(event_content,':')[1]) = 'np' then TRIM(SPLIT(event_content,':')[3]) 
                    else TRIM(SPLIT(event_content,':')[6]) END AS to_momo_id
                ,CASE 
                    WHEN TRIM(SPLIT(event_content,':')[1]) = 'np' then TRIM(SPLIT(event_content,':')[0]) 
                    else TRIM(SPLIT(event_content,':')[2]) END AS log_id
                ,user_agent
                ,row_number() over(distribute BY momo_id,event_content sort BY CAST(timestamp AS bigint) asc) rn 
            FROM online.bl_app_client_event
            WHERE partition_date = '${hivevar:partition_yesterday}' 
                AND partition_event_type='show' 
                AND partition_create_date = '${hivevar:partition_yesterday}' 
                AND (TRIM(SPLIT(event_content,':')[1]) = 'np' OR TRIM(SPLIT(event_content,':')[0]) = 'newnearbyuser')
                AND event_type = 'show' 
                AND length(momo_id) <= 10 
                AND length(momo_id) >= 5 
                AND momo_id != ''
        ) show_rn_tab
        WHERE rn = 1 AND length(to_momo_id) <= 10 AND length(to_momo_id) >= 5 AND to_momo_id != '' 
            AND length(from_momo_id) <= 10 AND length(from_momo_id) >= 5 AND from_momo_id != '' 
            AND log_id IS NOT NULL AND length(log_id) > 10
    ) event_show
    ON exposure_log.from_momo_id = event_show.from_momo_id 
                AND exposure_log.to_momo_id = event_show.to_momo_id 
                AND exposure_log.log_id = event_show.log_id 
    ) show_tab
    LEFT OUTER JOIN
    (
        SELECT
            from_momo_id
            ,to_momo_id
            ,log_id
            ,click_time
        FROM
        (
            SELECT 
                momo_id AS from_momo_id
                ,CASE 
                    WHEN TRIM(SPLIT(event_content,':')[1]) = 'np' then TRIM(SPLIT(event_content,':')[3]) 
                    else TRIM(SPLIT(event_content,':')[6]) END AS to_momo_id
                ,CASE 
                    WHEN TRIM(SPLIT(event_content,':')[1]) = 'np' then TRIM(SPLIT(event_content,':')[0]) 
                    else TRIM(SPLIT(event_content,':')[2]) END AS log_id
                ,row_number() over(distribute BY momo_id,event_content sort BY timestamp asc) rn 
                ,CAST(timestamp AS bigint) AS click_time
            FROM online.bl_app_client_event
            WHERE partition_date = '${hivevar:partition_yesterday}' 
                AND partition_event_type = 'click'
                AND partition_create_date = '${hivevar:partition_yesterday}'
                AND (TRIM(SPLIT(event_content,':')[1]) = 'np' OR TRIM(SPLIT(event_content,':')[0]) = 'newnearbyuser')
                AND event_type = 'click'
                AND length(momo_id) <= 10 
                AND length(momo_id) >= 5 
                AND momo_id != ''
        ) click_rn_tab
        WHERE rn = 1 AND length(to_momo_id) <= 10 AND length(to_momo_id) >= 5 AND to_momo_id != ''
    ) click_tab 
    ON show_tab.from_momo_id = click_tab.from_momo_id 
        AND show_tab.to_momo_id = click_tab.to_momo_id 
        AND show_tab.log_id = click_tab.log_id
) show_click_tab  
LEFT OUTER JOIN     
(
	SELECT
      from_momo_id
      ,to_momo_id
      ,sayhi_timestamp
      ,reply_timestamp
    FROM
    (
        SELECT
            basic.from_momo_id as from_momo_id
            ,basic.to_momo_id as to_momo_id
            ,basic.display_timestamp as sayhi_timestamp
            ,COALESCE(reply.display_timestamp, 0) as reply_timestamp
         --   ,reply.partition_date as partition_date
            ,row_number() over(distribute BY basic.from_momo_id, basic.to_momo_id, basic.display_timestamp sort BY reply.display_timestamp asc) rk 
      	FROM
        (
            SELECT 
                sender_id AS from_momo_id,
                receiver_id AS to_momo_id,
                cast(msg_timestamp AS int) AS display_timestamp
            FROM online.bl_msg_detail
            WHERE partition_date = '${hivevar:partition_yesterday}'
            AND partition_classification = 'p2p'
            AND msg_classification='p2p'
            AND is_first_sayhi = 1
            AND is_sayhi = 1
            AND sayhi_source_type = 1
        ) basic 
        left join 
        (
            SELECT 
                sender_id AS from_momo_id,
                receiver_id AS to_momo_id,
                cast(msg_timestamp AS int) AS display_timestamp
            FROM online.bl_msg_detail
            WHERE partition_date = '${hivevar:partition_date}'
            AND partition_classification = 'p2p'
            UNION all
            SELECT 
                sender_id AS from_momo_id,
                receiver_id AS to_momo_id,
                cast(msg_timestamp AS int) AS display_timestamp
            FROM online.bl_msg_detail
            WHERE partition_date = '${hivevar:partition_yesterday}'
            AND partition_classification = 'p2p'
        ) reply 
        on basic.from_momo_id = reply.to_momo_id and basic.to_momo_id = reply.from_momo_id
        where (basic.display_timestamp <= reply.display_timestamp and basic.display_timestamp + 86400 >= reply.display_timestamp) or reply.display_timestamp IS NULL
    ) basic_reply
    WHERE rk<2  
) hi_tab 
ON show_click_tab.from_momo_id = hi_tab.from_momo_id 
    AND show_click_tab.to_momo_id = hi_tab.to_momo_id 
where (show_click_tab.click_time-60 <= hi_tab.reply_timestamp and show_click_tab.click_time + 3600 >= hi_tab.reply_timestamp) or hi_tab.reply_timestamp=0 or hi_tab.reply_timestamp IS NULL
    
