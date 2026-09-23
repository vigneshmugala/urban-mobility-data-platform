{{config(materialized = 'table')}}


with source_data AS (   
    SELECT * FROM
    {{ref("stg_yellow_trips")}}
),

validated_data AS (
   SELECT * FROM source_data WHERE
   tpep_pickup_datetime IS NOT NULL AND 
   tpep_dropoff_datetime IS NOT NULL AND 
   trip_distance > 0 AND 
   tpep_dropoff_datetime > tpep_pickup_datetime AND 
   total_amount >= 0 AND
   passenger_count > 0
),
curated_new_fields_data AS (
    SELECT *, 
        timestampdiff(MINUTE,tpep_pickup_datetime,tpep_dropoff_datetime) AS trip_duration_minutes,
        DATE(tpep_pickup_datetime) AS pickup_date,
        HOUR(tpep_pickup_datetime) AS pickup_hour
    FROM validated_data
)

SELECT * FROM curated_new_fields_data