SELECT 
  PULocationID,
  pickup_date,
  COUNT(*)  as row_count

FROM {{ref("daily_metrics")}}
GROUP BY PULocationID, pickup_date
HAVING COUNT(*) > 1