-- Dimension table for NYC taxi zones
-- This is a simple pass-through from the seed file, but having it as a model
-- allows for future enhancements (e.g., adding calculated fields, filtering)

select
  "LocationID" as location_id,
  "Borough"    as borough,
  "Zone"       as zone,
  service_zone
from {{ ref('taxi_zone_lookup') }}