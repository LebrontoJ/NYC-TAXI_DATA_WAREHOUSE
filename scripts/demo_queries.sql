-- Top pickup zones by gross revenue
select
    pickup_borough,
    pickup_zone,
    sum(gross_revenue) as gross_revenue,
    sum(trip_count) as trip_count,
    sum(gross_revenue) / nullif(sum(trip_count), 0) as revenue_per_trip
from NYC_TAXI_DW.MARTS.MART_DAILY_REVENUE
group by 1, 2
order by gross_revenue desc
limit 20;

-- Hourly demand profile for airport trips
select
    pickup_hour_of_day,
    sum(trip_count) as trip_count,
    sum(airport_trip_count) as airport_trip_count,
    sum(airport_trip_count) / nullif(sum(trip_count), 0) as airport_trip_share
from NYC_TAXI_DW.MARTS.AGG_HOURLY_ZONE_DEMAND
group by 1
order by 1;

-- Zones with the highest late-night demand
select
    pickup_borough,
    pickup_zone,
    sum(trip_count) as late_night_trips,
    sum(gross_revenue) as gross_revenue
from NYC_TAXI_DW.MARTS.AGG_HOURLY_ZONE_DEMAND
where pickup_hour_of_day between 0 and 5
group by 1, 2
order by late_night_trips desc
limit 20;

