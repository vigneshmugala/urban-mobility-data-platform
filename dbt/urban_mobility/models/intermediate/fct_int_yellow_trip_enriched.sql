{{config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = '_record_hash'
)}}

with incremental_query AS (
    SELECT 
        * FROM
        {{ref("int_yellow_trip_taxi_zone_map")}} AS s


    -- INCREMENTAL condition
    {% if is_incremental() %}

    WHERE NOT EXISTS (
         SELECT 1
         FROM {{this}} AS t 
         WHERE t._source_file = s._source_file
    )

    {% endif %}
)

SELECT * FROM incremental_query