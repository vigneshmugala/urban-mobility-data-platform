SELECT
 pickup_date,
 pickup_hour,
 PULocationID,
 COUNT(*) as row_count

FROM {{ref("hourly_demand_metrics")}}
GROUP BY pickup_date, pickup_hour, PULocationID
HAVING COUNT(*) > 1