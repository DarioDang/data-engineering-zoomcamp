{{
  config(
    materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'  )
}}

-- Fact table containing all taxi trips enriched with zone information
-- This is a classic star schema design: fact table (trips) joined to dimension table (zones)
-- Materialized incrementally to handle large datasets efficiently

select
    -- Trip identifiers
    cast(trips.trip_id as text) as trip_id,
    cast(trips.vendor_id as integer) as vendor_id,
    cast(trips.service_type as text) as service_type,
    cast(trips.rate_code_id as integer) as rate_code_id,

    -- Location details
    cast(trips.pickup_location_id as integer) as pickup_location_id,
    cast(pz.borough as text) as pickup_borough,
    cast(pz.zone as text) as pickup_zone,

    cast(trips.dropoff_location_id as integer) as dropoff_location_id,
    cast(dz.borough as text) as dropoff_borough,
    cast(dz.zone as text) as dropoff_zone,

    -- Trip timing
    cast(trips.pickup_datetime as timestamp) as pickup_datetime,
    cast(trips.dropoff_datetime as timestamp) as dropoff_datetime,
    cast(trips.store_and_fwd_flag as text) as store_and_fwd_flag,

    -- Trip metrics
    cast(trips.passenger_count as integer) as passenger_count,
    cast(trips.trip_distance as numeric(10,2)) as trip_distance,
    cast(trips.trip_type as integer) as trip_type,
    cast(
        {{ get_trip_duration_minutes('trips.pickup_datetime', 'trips.dropoff_datetime') }}
        as bigint
    ) as trip_duration_minutes,

    -- Payment breakdown
    cast(trips.fare_amount as numeric(18,2)) as fare_amount,
    cast(trips.extra as numeric(18,2)) as extra,
    cast(trips.mta_tax as numeric(18,2)) as mta_tax,
    cast(trips.tip_amount as numeric(18,2)) as tip_amount,
    cast(trips.tolls_amount as numeric(18,2)) as tolls_amount,
    cast(trips.ehail_fee as numeric(18,2)) as ehail_fee,
    cast(trips.improvement_surcharge as numeric(18,2)) as improvement_surcharge,
    cast(trips.total_amount as numeric(18,2)) as total_amount,

    cast(trips.payment_type as integer) as payment_type,
    cast(trips.payment_type_description as text) as payment_type_description

from {{ ref('int_trips') }} as trips
left join {{ ref('dim_zones') }} as pz
    on trips.pickup_location_id = pz.location_id
left join {{ ref('dim_zones') }} as dz
    on trips.dropoff_location_id = dz.location_id

{% if is_incremental() %}
  -- Only process new trips based on pickup datetime
  where trips.pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}