/*
TO DO: 
 - One row per trip
 - Add a primary key (trip_id) 
 - Find all the duplicate rows and remove them
 - Find a way to enrich the column payment_type with seed data
*/

with trips AS (
    SELECT * 
    FROM {{ ref('int_trips_unioned') }}
),

trips_with_trip_id AS (
    -- Create unique trip ID for each trip --
    -- combine multiple columns to create a unique identifier for each trip --
    select 
        {{dbt_utils.surrogate_key([
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
        ]}} as trip_id,
        *
    from trips
    ),

deduped_trips AS (
    -- Remove duplicate rows --
    select 
        *
        except(row_num)
    from (
        select 
            *,
            row_number() over (partition by trip_id order by dropoff_datetime) as row_num
        from trips_with_trip_id
    )
    where row_num = 1
    ),

payment_type_lookup AS (
    -- Create a lookup for payment types --
    SELECT 
        CAST(payment_type AS int) AS payment_type,
        description
    FROM {{ ref('payment_type_lookup') }}
    
)

-- join the deduped trips with the payment type lookup to enrich the payment_type column --
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
    t.fare_amount,
    t.total_amount,
    t.payment_type,
    p.payment_type as payment_type_description
from 
    deduped_trips as t 
left join payment_type_lookup as p
on cast(t.payment_type as int) = p.payment_type

