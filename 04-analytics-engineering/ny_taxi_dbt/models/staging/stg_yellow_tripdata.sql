with source AS (
    SELECT * 
    FROM {{ source('nyc_raw_data','yellow_tripdata') }} 
),

renamed AS (

    SELECT 
        -- identifiers --
        CAST(vendorid AS int) AS vendor_id,
        CAST(ratecodeid AS int) AS rate_code_id,
        CAST(pulocationid AS int) AS pickup_location_id,
        CAST(dolocationid AS int) AS dropoff_location_id,

        -- timestamps --
        CAST(tpep_pickup_datetime AS timestamp) AS pickup_datetime,
        CAST(tpep_dropoff_datetime AS timestamp) AS dropoff_datetime,

        -- trip details --
        CAST(store_and_fwd_flag AS string) AS store_and_fwd_flag,
        CAST(passenger_count AS int) as passenger_count,
        CAST(trip_distance AS FLOAT64) AS trip_distance,

        -- payment details --
        CAST(fare_amount AS numeric) AS fare_amount, 
        CAST(extra AS numeric) AS extra,
        CAST(mta_tax AS numeric) AS mta_tax,
        CAST(tip_amount AS numeric) AS tip_amount,
        CAST(tolls_amount AS numeric) AS tolls_amount,
        CAST(improvement_surcharge AS numeric) AS improvement_surcharge,
        CAST(total_amount AS numeric) AS total_amount,
        CAST(payment_type AS int) AS payment_type,


    FROM {{ source('nyc_raw_data','yellow_tripdata') }}

    where 
        vendorid is not null 
        and improvement_surcharge >=0 
        -- Filter for 2019 and 2020 only (data quality requirement)
        and tpep_pickup_datetime is not null
        and tpep_dropoff_datetime is not null
        and tpep_pickup_datetime >= '2019-01-01'
        and tpep_pickup_datetime < '2021-01-01'
        and tpep_dropoff_datetime >= '2019-01-01'
        and tpep_dropoff_datetime <= '2021-01-01'
        -- Additional safety: ensure dropoff is after pickup
        and tpep_dropoff_datetime >= tpep_pickup_datetime
)

SELECT * FROM renamed
