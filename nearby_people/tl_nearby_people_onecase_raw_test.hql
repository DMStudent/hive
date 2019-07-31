ADD JAR hdfs://nameservice1/user/hive/warehouse/bin/udfs/momo-hive-latest.jar;
CREATE TEMPORARY FUNCTION from_json AS 'com.immomo.hive.common.udf.UDFFromJson';
CREATE TEMPORARY FUNCTION to_json AS 'com.immomo.hive.common.udf.UDFToJson';

ADD JAR hdfs://nameservice1/user/recommend/udfs/recommend-udfs-1.0.3-SNAPSHOT-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION getExperimentAssignment AS 'com.immomo.recommend.udfs.GetExperiment';

insert overwrite table recommend_nearby.tl_nearby_people_onecase_raw PARTITION (partition_date='${hivevar:partition_date}') 

select 
    show_click_tab.log_id as log_id
    ,show_click_tab.rec_time as rec_time
    ,show_click_tab.from_momo_id as from_momo_id
    ,show_click_tab.to_momo_id as to_momo_id
    ,show_click_tab.is_show as is_show
    ,show_click_tab.is_click
    ,COALESCE(hi_tab.has_send_first_sayhi, 0) as is_sayHi
    ,COALESCE(hi_tab.is_replyed_sayhi_today, 0) as is_reply
    ,show_click_tab.from_gender as from_gender
    ,show_click_tab.to_gender as to_gender
    ,show_click_tab.request_params as request_params
    ,show_click_tab.from_realtime_feature as from_realtime_feature
    ,show_click_tab.to_realtime_feature as to_realtime_feature
    ,show_click_tab.num_chat as num_chat
    ,show_click_tab.last_chat_time as last_chat_time
    ,show_click_tab.hour as hour
    ,show_click_tab.show_time as show_time
    ,show_click_tab.click_time as click_time
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
        ,show_tab.from_gender as from_gender
        ,show_tab.to_gender as to_gender
        ,show_tab.request_params as request_params
        ,show_tab.from_realtime_feature as from_realtime_feature
        ,show_tab.to_realtime_feature as to_realtime_feature
        ,show_tab.num_chat as num_chat
        ,show_tab.last_chat_time as last_chat_time
        ,show_tab.hour as hour
        ,show_tab.show_time as show_time
        ,click_tab.click_time as click_time
        ,show_tab.user_agent as user_agent
    from    
    (
    select 
        exposure_log.log_id as log_id
        ,exposure_log.rec_time as rec_time
        ,exposure_log.from_momo_id as from_momo_id
        ,exposure_log.to_momo_id as to_momo_id
        ,CASE WHEN event_show.from_momo_id IS NOT NULL AND event_show.to_momo_id IS NOT NULL AND event_show.log_id IS NOT NULL then 1 else 0 END AS is_show
        ,0 as is_click
        ,0 as is_sayHi
        ,0 as is_reply
        ,exposure_log.from_gender as from_gender
        ,exposure_log.to_gender as to_gender
        ,exposure_log.request_params as request_params
        ,exposure_log.from_realtime_feature as from_realtime_feature
        ,exposure_log.to_realtime_feature as to_realtime_feature
        ,exposure_log.num_chat as num_chat
        ,exposure_log.last_chat_time as last_chat_time
        ,event_show.hour as hour
        ,event_show.display_timestamp as show_time
        ,event_show.user_agent as user_agent
    from
    (
    select
        log_id
        ,rec_time
        ,from_momo_id
        ,exposure_map['momoId'] as to_momo_id
        ,'Male' as from_gender
        ,request_map['sex'] as to_gender
        ,concat_ws('-',request_map['expId'],request_map['from'],request_map['lat'],request_map['lng'],request_map['myMomoId'],request_map['pvId'],request_map['sage']) as request_params
        ,'unknow' as from_realtime_feature
        ,'unknow' as to_realtime_feature
        ,0 as num_chat
        ,0 as last_chat_time
        from 
        (
            select
                raw_map['requestId'] as log_id
                ,raw_map['momoId'] as from_momo_id
                ,raw_map['timeStamp'] as rec_time
                ,from_json(raw_map['exposureList'],'array<map<string,string>>') as exposure_list
                ,from_json(raw_map['request'],'map<string,string>') as request_map
                from 
                (
                    select
                        from_json(rawjson,'map<string,string>') as raw_map
                        ,partition_date
                    from recommend.bl_recommend_online_exposure_log 
                    where partition_date='${hivevar:partition_date}'
                ) t1 
        ) t2 lateral view explode(exposure_list) el as exposure_map 
    ) exposure_log 
    join
    (
        SELECT
        from_momo_id
        ,to_momo_id
        ,log_id
        ,hour
        ,display_timestamp
        ,user_agent
        FROM 
        (
            SELECT 
                momo_id AS from_momo_id
                ,hour(time) AS hour
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
            WHERE partition_date = '${hivevar:partition_date}' 
                AND partition_event_type='show' 
                AND partition_create_date = '${hivevar:partition_date}' 
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
            WHERE partition_date = '${hivevar:partition_date}' 
                AND partition_event_type = 'click'
                AND partition_create_date = '${hivevar:partition_date}'
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
        ,has_send_first_sayhi
        ,send_sayhi_num
        ,is_replyed_sayhi_today
        ,send_msg_num
        ,receive_msg_num
        ,read_msg_event_num
    FROM online.ml_user_pair_msg_theme
    WHERE partition_date = '${hivevar:partition_date}'
        AND has_send_first_sayhi=1
        AND first_sayhi_source_type=1
        AND length(from_momo_id) <= 12
        AND length(to_momo_id) <= 12
        AND from_momo_id != '' AND to_momo_id != ''
) hi_tab 
ON show_click_tab.from_momo_id = hi_tab.from_momo_id 
    AND show_click_tab.to_momo_id = hi_tab.to_momo_id 
                     