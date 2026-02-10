with source AS (
    SELECT * 
    FROM {{ source('nyc_raw_data','fhv_green_tripdata') }} 
),

renamed as (
    select
        -- identifiers 
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,
        cast(dispatching_base_num as string) as dispatching_base_num,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(SR_Flag as numeric) as sr_flag,
        cast(Affiliated_base_number as string) as affiliated_base_number,
    
    from source
    -- Filter out records with null dispatching_base_num
    where dispatching_base_num is not null
)

-- Final selection
select * from renamed