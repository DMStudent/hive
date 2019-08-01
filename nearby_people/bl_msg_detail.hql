SELECT
	*
FROM
(
	SELECT
        basic.from_momo_id as from_momo_id
        ,basic.to_momo_id as to_momo_id
        ,basic.display_timestamp as sayhi_timestamp
        ,reply.display_timestamp as reply_timestamp
        ,reply.partition_date as partition_date
        ,row_number() over(distribute BY basic.from_momo_id, basic.to_momo_id, basic.display_timestamp sort BY reply.display_timestamp asc) rk 
    FROM
    (
        SELECT 
            partition_date,
            sender_id AS from_momo_id,
            receiver_id AS to_momo_id,
            cast(msg_timestamp AS int) AS display_timestamp
        FROM online.bl_msg_detail
        WHERE partition_date = 20190730
        AND partition_classification = 'p2p'
        AND msg_classification='p2p'
        AND is_first_sayhi = 1
        AND is_sayhi = 1
        AND sayhi_source_type = 1
        LIMIT 100
    ) basic 
    join 
    (
        SELECT 
            partition_date,
            sender_id AS from_momo_id,
            receiver_id AS to_momo_id,
            cast(msg_timestamp AS int) AS display_timestamp
        FROM online.bl_msg_detail
        WHERE partition_date = 20190730
        UNION all
        SELECT 
            partition_date,
            sender_id AS from_momo_id,
            receiver_id AS to_momo_id,
            cast(msg_timestamp AS int) AS display_timestamp
        FROM online.bl_msg_detail
        WHERE partition_date = 20190729

    ) reply 
    on basic.from_momo_id = reply.to_momo_id and basic.to_momo_id = reply.from_momo_id
    where basic.display_timestamp <= reply.display_timestamp and basic.display_timestamp + 86400 >= reply.display_timestamp
) basic_reply
WHERE rk<2
-- group by basic.partition_date, basic.test_type, basic.from_momo_id, basic.to_momo_id,basic.display_timestam





SELECT 
    partition_date,
    momo_id AS from_momo_id,
    getExperimentAssignment(momo_id,'${hivevar:nearbyPeopleLayerV2}', partition_date) AS test_type,
    extend_map['to_momo_id'] AS to_momo_id,
    cast(display_timestamp AS int) AS display_timestamp
FROM online.bl_user_uniform_event_detail
WHERE partition_date = '${hivevar:partition_date}'
AND business_type='follow'
AND event_name='follow.default'
AND extend_map['source_type'] =2
UNION ALL

SELECT
    basic.from_momo_id
    basic.to_momo_id
    basic.display_timestam as sayhi_timestam
    reply.display_timestam as reply_timestam
(
    SELECT 
        partition_date,
        sender_id AS from_momo_id,
        getExperimentAssignment(sender_id,'${hivevar:nearbyPeopleLayerV2}', partition_date) AS test_type,
        receiver_id AS to_momo_id,
        cast(msg_timestamp AS int) AS display_timestamp
    FROM online.bl_msg_detail
    WHERE partition_date = '${hivevar:partition_date}'
    AND partition_classification = 'p2p'
    AND msg_classification='p2p'
    AND is_first_sayhi = 1
    AND is_sayhi = 1
    AND sayhi_source_type = 1
)basic 
join (
    SELECT 
        partition_date,
        sender_id AS from_momo_id,
        receiver_id AS to_momo_id,
        cast(msg_timestamp AS int) AS display_timestamp
    FROM online.bl_msg_detail
    WHERE partition_date = '${hivevar:partition_date}'
)reply 
on basic.from_momo_id = reply.to_momo_id and basic.to_momo_id = reply.from_momo_id
where basic.display_timestamp <= reply.display_timestamp and basic.display_timestamp + 86400 >= reply.display_timestamp
group by basic.partition_date, basic.test_type, basic.from_momo_id, basic.to_momo_id,basic.display_timestam

(
    SELECT 
        partition_date,
        momo_id AS from_momo_id,
        getExperimentAssignment(momo_id,'${hivevar:nearbyPeopleLayerV2}', partition_date) AS test_type,
        extend_map['to_momo_id'] AS to_momo_id,
        cast(display_timestamp AS int) AS display_timestamp
    FROM online.bl_user_uniform_event_detail
    WHERE partition_date = '${hivevar:partition_date}'
    AND business_type='follow'
    AND event_name='follow.default'
    AND extend_map['source_type'] =2
    UNION ALL
    SELECT 
        partition_date,
        sender_id AS from_momo_id,
        getExperimentAssignment(sender_id,'${hivevar:nearbyPeopleLayerV2}', partition_date) AS test_type,
        receiver_id AS to_momo_id,
        cast(msg_timestamp AS int) AS display_timestamp
    FROM online.bl_msg_detail
    WHERE partition_date = '${hivevar:partition_date}'
    AND partition_classification = 'p2p'
    AND msg_classification='p2p'
    AND is_first_sayhi = 1
    AND is_sayhi = 1
    AND sayhi_source_type = 1
)basic 
join 
(
    SELECT 
        partition_date,
        sender_id AS from_momo_id,
        receiver_id AS to_momo_id,
        cast(msg_timestamp AS int) AS display_timestamp
    FROM online.bl_msg_detail
    WHERE partition_date = '${hivevar:partition_date}'
)reply 
on basic.from_momo_id = reply.to_momo_id and basic.to_momo_id = reply.from_momo_id
where basic.display_timestamp <= reply.display_timestamp and basic.display_timestamp + 86400 >= reply.display_timestamp
group by basic.partition_date, basic.test_type, basic.from_momo_id, basic.to_momo_id,basic.display_timestam