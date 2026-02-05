with taxi_zone_lookup as (
    SELECT *  
    FROM {{ ref('taxi_zone_lookup') }}
)

SELECT * 
FROM taxi_zone_lookup