select
    pickup_hour,
    pickup_date,
    pickup_hour_of_day,
    pickup_day_name,
    pickup_location_id,
    pickup_borough,
    pickup_zone,
    count(*) as trip_count,
    sum(total_amount) as gross_revenue,
    avg(total_amount) as avg_total_amount,
    avg(trip_distance) as avg_trip_distance,
    avg(trip_duration_minutes) as avg_trip_duration_minutes,
    count_if(is_airport_trip) as airport_trip_count
from {{ ref('fct_taxi_trips') }}
group by 1, 2, 3, 4, 5, 6, 7

