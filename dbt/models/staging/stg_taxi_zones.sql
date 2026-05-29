select
    locationid::number as location_id,
    borough::string as borough,
    zone::string as zone,
    service_zone::string as service_zone
from {{ ref('taxi_zone_lookup') }}

