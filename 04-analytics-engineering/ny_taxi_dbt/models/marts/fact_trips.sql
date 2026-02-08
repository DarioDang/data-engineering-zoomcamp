{{ config(
    materialized='incremental',
    unique_key= 'trip_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    partition_by={
      "field": "dropoff_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    }
)}}

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
        description as payment_type_description
    from {{ ref('payment_type_lookup') }}
)

-- Final: join description onto trips
select
    -- Trip identifiers
    t.trip_id,
    t.vendor_id,
    t.service_type,
    t.rate_code_id,

    -- Location details (enriched with human-readable zone names from dimension)
    t.pickup_location_id,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    t.dropoff_location_id,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,

    -- Trip timing
    t.pickup_datetime,
    t.dropoff_datetime,
    t.store_and_fwd_flag,

    -- Trip metrics
    t.passenger_count,
    cast(t.trip_distance as numeric) as trip_distance,
    t.trip_type,

    -- Payment breakdown
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.ehail_fee,
    t.improvement_surcharge,
    t.total_amount,
    t.payment_type,
    p.payment_type_description
from deduped_trips t
left join payment_type_lookup p
    on cast(t.payment_type as int64) = p.payment_type
left join {{ ref('dim_zones') }} as pz
    on t.pickup_location_id = pz.location_id
left join {{ ref('dim_zones') }} as dz
    on t.dropoff_location_id = dz.location_id
