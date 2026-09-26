{% snapshot taxi_zone_history %}

{{config(
   target_schema = 'silver',
   unique_key = 'LocationID',
   strategy = 'check',
   check_cols = ['Borough', 'Zone', 'service_zone'],

   snapshot_meta_column_names = {
    'dbt_valid_from' : 'valid_from',
    'dbt_valid_to' : 'valid_till'
   }
)}}


SELECT LocationID, Borough, Zone, service_zone FROM {{source('bronze', 'taxi_lookup_TEST')}}

{% endsnapshot %}