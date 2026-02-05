with taxi_zone_lookup as (
    SELECT *  
    FROM {{ ref('taxi_zone_lookup') }}
),

renamed AS (
    SELECT
        locationID as location_id,
        borough,
        zone,
        service_zone
    FROM taxi_zone_lookup
)

SELECT * 
FROM renamed