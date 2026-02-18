/* @bruin
name: reports.trips_report
type: pg.sql
connection: pg-zoomcamp

depends:
  - staging.trips

materialization:
  type: table
  strategy: create+replace

columns:
  - name: pickup_date
    type: date
    description: Date of pickup (truncated from pickup_datetime)
    primary_key: true
    checks:
      - name: not_null
  - name: service_type
    type: varchar
    description: Taxi service type (green or yellow)
    primary_key: true
    checks:
      - name: not_null
  - name: total_trips
    type: bigint
    description: Number of trips
    checks:
      - name: not_null
      - name: non_negative
  - name: total_passengers
    type: double precision
    description: Total passenger count
    checks:
      - name: non_negative
  - name: avg_trip_distance_miles
    type: double precision
    description: Average trip distance in miles
    checks:
      - name: non_negative
  - name: avg_trip_duration_minutes
    type: numeric
    description: Average trip duration in minutes
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: double precision
    description: Sum of base fare amounts
    checks:
      - name: non_negative
  - name: total_tip_amount
    type: double precision
    description: Sum of tip amounts
    checks:
      - name: non_negative
  - name: total_revenue
    type: double precision
    description: Sum of total_amount charged to passengers
    checks:
      - name: not_null
      - name: non_negative
  - name: avg_revenue_per_trip
    type: double precision
    description: Average total amount per trip
    checks:
      - name: non_negative

@bruin */

SELECT
    pickup_datetime::date                                   AS pickup_date,
    service_type,
    COUNT(*)                                                AS total_trips,
    SUM(passenger_count)                                    AS total_passengers,
    ROUND(AVG(trip_distance)::numeric, 2)                   AS avg_trip_distance_miles,
    ROUND(AVG(trip_duration_minutes), 2)                    AS avg_trip_duration_minutes,
    ROUND(SUM(fare_amount)::numeric, 2)                     AS total_fare_amount,
    ROUND(SUM(tip_amount)::numeric, 2)                      AS total_tip_amount,
    ROUND(SUM(total_amount)::numeric, 2)                    AS total_revenue,
    ROUND(AVG(total_amount)::numeric, 2)                    AS avg_revenue_per_trip
FROM staging.trips
GROUP BY
    pickup_datetime::date,
    service_type
ORDER BY
    pickup_date,
    service_type