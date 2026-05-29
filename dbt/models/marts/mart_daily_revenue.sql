select
    pickup_date,
    pickup_borough,
    pickup_zone,
    count(*) as trip_count,
    count_if(is_airport_trip) as airport_trip_count,
    sum(total_amount) as gross_revenue,
    sum(fare_amount) as fare_revenue,
    sum(tip_amount) as tip_revenue,
    avg(tip_pct_of_fare) as avg_tip_pct_of_fare,
    avg(trip_distance) as avg_trip_distance,
    avg(trip_duration_minutes) as avg_trip_duration_minutes,
    avg(revenue_per_mile) as avg_revenue_per_mile
from {{ ref('fct_taxi_trips') }}
group by 1, 2, 3
