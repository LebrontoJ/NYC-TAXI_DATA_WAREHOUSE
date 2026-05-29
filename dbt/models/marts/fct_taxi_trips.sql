with trips as (
    select *
    from {{ ref('stg_yellow_taxi_trips') }}
    qualify row_number() over (
        partition by trip_id
        order by loaded_at desc, source_file desc
    ) = 1
),

zones as (
    select * from {{ ref('dim_zones') }}
)

select
    trips.trip_id,
    trips.vendor_id,
    trips.pickup_at,
    trips.dropoff_at,
    date_trunc('hour', trips.pickup_at) as pickup_hour,
    to_date(trips.pickup_at) as pickup_date,
    extract(hour from trips.pickup_at) as pickup_hour_of_day,
    dayname(trips.pickup_at) as pickup_day_name,
    datediff('minute', trips.pickup_at, trips.dropoff_at) as trip_duration_minutes,
    trips.passenger_count,
    trips.trip_distance,
    trips.rate_code_id,
    trips.payment_type,
    trips.pickup_location_id,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    pickup_zone.service_zone as pickup_service_zone,
    trips.dropoff_location_id,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    dropoff_zone.service_zone as dropoff_service_zone,
    trips.fare_amount,
    trips.extra_amount,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.improvement_surcharge,
    trips.congestion_surcharge,
    trips.airport_fee,
    trips.total_amount,
    trips.tip_amount / nullif(trips.fare_amount, 0) as tip_pct_of_fare,
    trips.total_amount / nullif(trips.trip_distance, 0) as revenue_per_mile,
    coalesce(pickup_zone.is_airport, false) or coalesce(dropoff_zone.is_airport, false) as is_airport_trip,
    trips.source_file,
    trips.source_row_number,
    trips.loaded_at
from trips
left join zones as pickup_zone
    on trips.pickup_location_id = pickup_zone.location_id
left join zones as dropoff_zone
    on trips.dropoff_location_id = dropoff_zone.location_id
