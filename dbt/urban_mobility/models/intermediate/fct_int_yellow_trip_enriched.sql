{{config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = '_record_hash'
)}}

with incremental_query AS (
    SELECT 
        * FROM
        {{ref("int_yellow_trip_taxi_zone_map")}}


    -- INCREMENTAL condition
    {% if is_incremental() %}
    WHERE 
        _source_month > (SELECT MAX(_source_month) FROM {{this}})

    {% endif %}
)

SELECT * FROM incremental_query