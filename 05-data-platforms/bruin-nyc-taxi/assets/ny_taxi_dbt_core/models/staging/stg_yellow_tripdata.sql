{{
    config(
        materialized='view',
    )
}}

with source as (
    select * from {{ source('raw', 'yellow_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast("VendorID" as integer) as vendor_id,
        cast("RatecodeID" as integer) as rate_code_id,
        cast("PULocationID" as integer) as pickup_location_id,
        cast("DOLocationID" as integer) as dropoff_location_id,

        -- timestamps
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(store_and_fwd_flag as varchar) as store_and_fwd_flag,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,

        -- payment info
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        cast(payment_type as integer) as payment_type,

        -- metadata
        cast(ingestion_year as integer) as ingestion_year,
        cast(ingestion_month as integer) as ingestion_month

    from source
    where "VendorID" is not null
)

select * from renamed
