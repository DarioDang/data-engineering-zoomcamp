with trips_unioned as (
    select
        *
    from {{ ref('int_trips_unioned') }}
),

vendors AS (
    select 
        distinct vendor_id,
        {{ get_vendor_names('vendor_id') }} as vendor_name
    from 
        trips_unioned
)

SELECT *
FROM vendors;