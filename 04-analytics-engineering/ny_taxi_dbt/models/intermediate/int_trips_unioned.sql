-- Uniion grren & yellow taxi data into a single dataset 

-- CTE table for green taxi data --
with green_trips as (
    SELECT 
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        'green' AS service_type
    FROM {{ ref('stg_green_tripdata') }}
),

-- CTE table for yellow taxi data --
with yellow_trips as (
    SELECT 
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        1 AS trip_type, -- Yellow taxis only do street-hail (code 1)
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        CAST(0 AS numertic) AS ehail_fee, -- yellow taxi data doesn't have ehail fee
        improvement_surcharge,
        total_amount,
        payment_type,
        'yellow' AS service_type
    FROM {{ ref('stg_yellow_tripdata') }}
)

-- Union --
select * 
from green_trips

UNION ALL

select * 
from yellow_trips