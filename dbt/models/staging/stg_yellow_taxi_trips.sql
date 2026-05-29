with raw_trips as (
    select
        src,
        source_file,
        source_row_number,
        loaded_at
    from {{ source('raw', 'YELLOW_TAXI_TRIPS') }}
),

typed as (
    select
        src:"VendorID"::number as vendor_id,
        src:"tpep_pickup_datetime"::timestamp_ntz as pickup_at,
        src:"tpep_dropoff_datetime"::timestamp_ntz as dropoff_at,
        src:"passenger_count"::number as passenger_count,
        src:"trip_distance"::float as trip_distance,
        src:"RatecodeID"::number as rate_code_id,
        src:"store_and_fwd_flag"::string as store_and_fwd_flag,
        src:"PULocationID"::number as pickup_location_id,
        src:"DOLocationID"::number as dropoff_location_id,
        src:"payment_type"::number as payment_type,
        src:"fare_amount"::float as fare_amount,
        src:"extra"::float as extra_amount,
        src:"mta_tax"::float as mta_tax,
        src:"tip_amount"::float as tip_amount,
        src:"tolls_amount"::float as tolls_amount,
        src:"improvement_surcharge"::float as improvement_surcharge,
        src:"total_amount"::float as total_amount,
        src:"congestion_surcharge"::float as congestion_surcharge,
        src:"Airport_fee"::float as airport_fee,
        source_file,
        source_row_number,
        loaded_at
    from raw_trips
),

cleaned as (
    select
        md5(coalesce(source_file, '') || '|' || coalesce(source_row_number::string, '')) as trip_id,
        *
    from typed
    where pickup_at is not null
      and dropoff_at is not null
      and pickup_at < dropoff_at
      and trip_distance between 0 and 100
      and total_amount between 0 and 1000
)

select * from cleaned
