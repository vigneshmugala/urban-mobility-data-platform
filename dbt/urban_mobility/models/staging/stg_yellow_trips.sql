{{config(
    materialized = 'table'
)}}


SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
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
    cbd_congestion_fee,

    _record_hash,
    _source_file,
    _source_month,
    _ingested_at,
    _ingestion_batch_id
FROM
{{source('bronze', 'yellow_trips_raw')}}