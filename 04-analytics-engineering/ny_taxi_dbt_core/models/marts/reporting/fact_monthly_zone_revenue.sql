-- Data mart for monthly revenue analysis by pickup zone and service type
-- This aggregation is optimized for business reporting and dashboards
-- Enables analysis of revenue trends across different zones and taxi types

select
    -- Grouping dimensions
    coalesce(pickup_zone, 'Unknown Zone') as pickup_zone,
    date_trunc('month', pickup_datetime) as revenue_month,
    service_type,

    -- Revenue breakdown (summed by zone, month, and service type)
    sum(fare_amount) as revenue_monthly_fare,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly_mta_tax,
    sum(tip_amount) as revenue_monthly_tip_amount,
    sum(tolls_amount) as revenue_monthly_tolls_amount,
    sum(ehail_fee) as revenue_monthly_ehail_fee,
    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total_amount,

    -- Additional metrics for operational analysis
    count(trip_id) as total_monthly_trips,
    cast(round(avg(passenger_count), 2) as numeric(10,2)) as avg_monthly_passenger_count,
    cast(round(avg(trip_distance), 2) as numeric(10,2)) as avg_monthly_trip_distance

from {{ ref('fact_trips') }}
group by pickup_zone, revenue_month, service_type