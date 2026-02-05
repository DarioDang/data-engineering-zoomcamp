SELECT *
FROM {{ source('nyc_raw_data','green_tripdata') }}