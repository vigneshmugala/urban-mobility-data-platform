{{config(
    materialized = 'table'
)}}

SELECT 
   _record_hash,
    VendorID,

    tpep_pickup_datetime,
    tpep_dropoff_datetime,

    pickup_date,
    pickup_hour,
    trip_duration_minutes,

    passenger_count,
    trip_distance,

    PULocationID,
    DOLocationID,

    pickup_borough,
    pickup_zone,
    pickup_service_zone,

    drop_borough,
    drop_zone,
    drop_service_zone,

    payment_type,

    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee,
    cbd_congestion_fee
FROM {{ref('fct_int_yellow_trip_enriched')}}