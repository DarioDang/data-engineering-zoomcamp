SELECT 
    -- identifiers --
    CAST(vendorid AS int) AS vendor_id,
    CAST(ratecodeid AS int) AS rate_code_id,
    CAST(pulocationid AS int) AS pickup_location_id,
    CAST(dolocationid AS int) AS dropoff_location_id,

    -- timestamps --
    CAST(lpep_pickup_datetime AS timestamp) AS pickup_datetime,
    CAST(lpep_dropoff_datetime AS timestamp) AS dropoff_datetime,

    -- trip details --
    CAST(store_and_fwd_flag AS string) AS store_and_fwd_flag,
    CAST(passenger_count AS int) as passenger_count,
    CAST(trip_distance AS FLOAT64) AS trip_distance,
    CAST(trip_type as int) AS trip_type, 

    -- payment details --
    CAST(fare_amount AS numeric) AS fare_amount, 
    CAST(extra AS numeric) AS extra,
    CAST(mta_tax AS numeric) AS mta_tax,
    CAST(tip_amount AS numeric) AS tip_amount,
    CAST(tolls_amount AS numeric) AS tolls_amount,
    CAST(improvement_surcharge AS numeric) AS improvement_surcharge,
    CAST(total_amount AS numeric) AS total_amount,
    CAST(payment_type AS int) AS payment_type,


FROM {{ source('nyc_raw_data','green_tripdata') }}

WHERE vendorid IS NOT NULL
