select
    location_id,
    borough,
    zone,
    service_zone,
    case
        when zone in ('JFK Airport', 'LaGuardia Airport', 'Newark Airport') then true
        else false
    end as is_airport
from {{ ref('stg_taxi_zones') }}

