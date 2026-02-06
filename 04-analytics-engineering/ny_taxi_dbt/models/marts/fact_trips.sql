{{ config(materialized='table') }}

-- ✅ Goal:
-- 1) Take all trips from int_trips_unioned
-- 2) Create a unique trip_id
-- 3) Remove duplicates (keep only 1 row per trip_id)
-- 4) Join payment_type description from seed table

with trips as (
    -- Step 1: get all trips (green + yellow already unioned)
    select *
    from {{ ref('int_trips_unioned') }}
),

trips_with_trip_id as (
    -- Step 2: create a unique ID for each trip
    -- We combine multiple columns to "identify" a trip
    select
        {{ dbt_utils.generate_surrogate_key([
            "service_type",
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_location_id",
            "dropoff_location_id",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount"
        ]) }} as trip_id,
        *
    from trips
),

deduped_trips as (
    -- Step 3: remove duplicate rows
    -- If there are duplicates (same trip_id), keep the latest dropoff_datetime row
    select * except(row_num)
    from (
        select
            *,
            row_number() over (
                partition by trip_id
                order by dropoff_datetime desc
            ) as row_num
        from trips_with_trip_id
    )
    where row_num = 1
),

payment_type_lookup as (
    -- Step 4: payment_type seed table (1=Card, 2=Cash, ...)
    select
        cast(payment_type as int64) as payment_type,
        payment_type_description
    from {{ ref('payment_type') }}
)

-- Final: join description onto trips
select
    t.trip_id,
    t.service_type,
    t.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.total_amount,
    t.payment_type,
    p.payment_type_description
from deduped_trips t
left join payment_type_lookup p
    on cast(t.payment_type as int64) = p.payment_type
