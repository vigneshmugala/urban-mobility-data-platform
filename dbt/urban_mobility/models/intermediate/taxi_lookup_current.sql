{{config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = 'LocationID'
)}}

SELECT * FROM {{source('bronze', 'taxi_lookup_table')}}