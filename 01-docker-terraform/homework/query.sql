select count(*) from green_taxi_data gtd ;

select * from taxi_zone_lookup tzl ;



SELECT count(1)
FROM green_taxi_data
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;



SELECT 
    CAST(lpep_pickup_datetime AS DATE) AS pickup_day,
    MAX(trip_distance) AS max_distance
FROM 
    green_taxi_data
WHERE 
    trip_distance < 100
GROUP BY 
    pickup_day
ORDER BY 
    max_distance DESC
LIMIT 1000;


SELECT 
    z."Zone", 
    SUM(t."total_amount") AS total_sum
FROM 
    green_taxi_data t
JOIN 
    taxi_zone_lookup z ON t."PULocationID" = z."LocationID"
WHERE 
    CAST(t.lpep_pickup_datetime AS DATE) = '2025-11-18'
GROUP BY 
    z."Zone"
ORDER BY 
    total_sum DESC
LIMIT 1;


SELECT 
    zdo."Zone" AS dropoff_zone,
    t.tip_amount
FROM 
    green_taxi_data t
JOIN 
    taxi_zone_lookup zpu ON t."PULocationID" = zpu."LocationID"
JOIN 
    taxi_zone_lookup zdo ON t."DOLocationID" = zdo."LocationID"
WHERE 
    zpu."Zone" = 'East Harlem North'
    AND t.lpep_pickup_datetime >= '2025-11-01' 
    AND t.lpep_pickup_datetime < '2025-12-01'
ORDER BY 
    t.tip_amount DESC
LIMIT 1;