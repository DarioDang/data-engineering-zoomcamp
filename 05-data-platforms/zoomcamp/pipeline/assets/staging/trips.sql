/* @bruin
name: staging.trips
type: pg.sql
connection: pg-zoomcamp

depends:
  - ingestion.trips
  - raw.payment_lookup

materialization:
  type: table
  strategy: create+replace

columns:
  - name: trip_id
    type: varchar
    description: Surrogate key (md5 of vendor + service + pickup_datetime + locations)
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: service_type
    type: varchar
    description: Taxi service type (green or yellow)
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    description: Pickup timestamp
    checks:
      - name: not_null
  - name: total_amount
    type: double precision
    description: Total amount charged to passengers
    checks:
      - name: not_null
      - name: non_negative

custom_checks:
  - name: row_count_positive
    description: Ensure staging table has at least one row
    query: SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM staging.trips
    value: 1

@bruin */

WITH green AS (
    SELECT
        vendor_id::text                         AS vendor_id,
        'green'                                 AS service_type,
        pickup_datetime::timestamp              AS pickup_datetime,
        dropoff_datetime::timestamp             AS dropoff_datetime,
        store_and_fwd_flag::text               AS store_and_fwd_flag,
        rate_code_id::double precision         AS rate_code_id,
        pickup_location_id::double precision   AS pickup_location_id,
        dropoff_location_id::double precision  AS dropoff_location_id,
        passenger_count::double precision      AS passenger_count,
        trip_distance::double precision        AS trip_distance,
        trip_type::double precision            AS trip_type,
        fare_amount::double precision          AS fare_amount,
        extra::double precision                AS extra,
        mta_tax::double precision              AS mta_tax,
        tip_amount::double precision           AS tip_amount,
        tolls_amount::double precision         AS tolls_amount,
        ehail_fee::double precision            AS ehail_fee,
        improvement_surcharge::double precision AS improvement_surcharge,
        congestion_surcharge::double precision  AS congestion_surcharge,
        NULL::double precision                 AS airport_fee,
        total_amount::double precision         AS total_amount,
        payment_type::double precision         AS payment_type,
        extracted_at::timestamp                AS extracted_at
    FROM raw.green_tripdata
    WHERE vendor_id IS NOT NULL
      AND total_amount > 0
      AND trip_distance >= 0
),

yellow AS (
    SELECT
        vendor_id::text                         AS vendor_id,
        'yellow'                                AS service_type,
        pickup_datetime::timestamp              AS pickup_datetime,
        dropoff_datetime::timestamp             AS dropoff_datetime,
        store_and_fwd_flag::text               AS store_and_fwd_flag,
        rate_code_id::double precision         AS rate_code_id,
        pickup_location_id::double precision   AS pickup_location_id,
        dropoff_location_id::double precision  AS dropoff_location_id,
        passenger_count::double precision      AS passenger_count,
        trip_distance::double precision        AS trip_distance,
        NULL::double precision                 AS trip_type,
        fare_amount::double precision          AS fare_amount,
        extra::double precision                AS extra,
        mta_tax::double precision              AS mta_tax,
        tip_amount::double precision           AS tip_amount,
        tolls_amount::double precision         AS tolls_amount,
        NULL::double precision                 AS ehail_fee,
        improvement_surcharge::double precision AS improvement_surcharge,
        congestion_surcharge::double precision  AS congestion_surcharge,
        airport_fee::double precision          AS airport_fee,
        total_amount::double precision         AS total_amount,
        payment_type::double precision         AS payment_type,
        extracted_at::timestamp                AS extracted_at
    FROM raw.yellow_tripdata
    WHERE vendor_id IS NOT NULL
      AND total_amount > 0
      AND trip_distance >= 0
),

unioned AS (
    SELECT * FROM green
    UNION ALL
    SELECT * FROM yellow
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY
                vendor_id,
                service_type,
                pickup_datetime,
                pickup_location_id,
                dropoff_location_id
            ORDER BY extracted_at DESC
        ) AS rn
    FROM unioned
)

SELECT
    md5(
        COALESCE(d.vendor_id::text, '') || '-' ||
        d.service_type || '-' ||
        d.pickup_datetime::text || '-' ||
        COALESCE(d.pickup_location_id::text, '') || '-' ||
        COALESCE(d.dropoff_location_id::text, '')
    )  AS trip_id,
    d.vendor_id,
    d.service_type,
    d.pickup_datetime,
    d.dropoff_datetime,
    ROUND((EXTRACT(EPOCH FROM (d.dropoff_datetime - d.pickup_datetime)) / 60)::numeric, 2)
    AS trip_duration_minutes,
    d.store_and_fwd_flag,
    d.rate_code_id,
    d.pickup_location_id,
    d.dropoff_location_id,
    d.passenger_count,
    d.trip_distance,
    d.trip_type,
    d.fare_amount,
    d.extra,
    d.mta_tax,
    d.tip_amount,
    d.tolls_amount,
    d.ehail_fee,
    d.improvement_surcharge,
    d.congestion_surcharge,
    d.airport_fee,
    d.total_amount,
    d.payment_type,
    pl.payment_type_name,
    d.extracted_at
FROM deduped d
LEFT JOIN raw.payment_lookup pl
    ON d.payment_type = pl.payment_type_id
WHERE d.rn = 1
