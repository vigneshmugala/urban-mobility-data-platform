{{config(materialized = 'table')}}


WITH clean_data AS (
  SELECT * FROM {{ref("int_yellow_trip_cleaned")}}
),
pickup_taxi_lookup AS (
    SELECT * FROM {{source('bronze' , 'taxi_lookup_table')}}
),
drop_taxi_lookup AS (
    SELECT * FROM {{source('bronze', 'taxi_lookup_table')}}
)

SELECT
  ct.*,
  p.Borough as pickup_borough,
  p.zone as pickup_zone,
  p.service_zone as pickup_service_zone,

  d.Borough as drop_borough,
  d.zone as drop_zone,
  d.service_zone as drop_service_zone

  FROM
    clean_data ct 
    LEFT JOIN pickup_taxi_lookup p ON ct.PULocationID = p.LocationID
    LEFT JOIN drop_taxi_lookup d ON ct.DOLocationID = d.LocationID


