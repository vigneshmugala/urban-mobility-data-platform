{{config(materialized = 'view')}}

with daily_analytics AS (
   SELECT 
      pickup_date,
      PULocationID,
      COUNT(*) as total_trips,
      SUM(passenger_count) as total_passenger_count,
      SUM(trip_distance) as total_trip_distance,
      SUM(fare_amount) as total_fare_amount,
      SUM(tip_amount) as total_tip_amount,
      SUM(total_amount) as total_amount_earned,
      SUM(trip_duration_minutes) as total_duration_travel,
      AVG(passenger_count) as average_passenger_count,
      AVG(trip_distance) as average_trip_distance,
      AVG(fare_amount) as average_fare_amount,
      AVG(tip_amount) as average_tip_amount,
      AVG(total_amount) as average_amount_earned,
      AVG(trip_duration_minutes) as average_trip_duration_travel

   FROM {{ref('fct_trips')}}    
     GROUP BY
        pickup_date,PULocationID
)


SELECT  
   m.pickup_date,
   m.PULocationID,

   z.Borough,
   z.Zone,
   z.service_zone,
   
   m.total_trips,
   m.total_passenger_count,
   m.total_trip_distance,
   m.total_fare_amount,
   m.total_tip_amount,
   m.total_amount_earned,
   m.total_duration_travel,
   m.average_passenger_count,
   m.average_trip_distance,
   m.average_fare_amount,
   m.average_tip_amount,
   m.average_amount_earned,
   m.average_trip_duration_travel
  
FROM daily_analytics m 
LEFT JOIN {{ref("dim_taxi_zone")}} z  ON m.PULocationID = z.LocationID
 