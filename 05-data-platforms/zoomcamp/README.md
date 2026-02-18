# NYC Taxi Data Pipeline

## Overview

This pipeline ingests, transforms, and aggregates NYC Taxi & Limousine Commission (TLC) trip data using [Bruin](https://github.com/bruin-data/bruin) as the orchestration framework with PostgreSQL as the data warehouse.

**Pipeline name:** `nyc-taxi`
**Schedule:** Daily
**Data source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (Parquet files)
**Destination:** PostgreSQL (`nyc_taxi` database)

---

## Architecture

```
                          NYC TLC Public S3
                         (Parquet files)
                               |
                               v
                   +-----------------------+
                   |   INGESTION LAYER     |
                   |-----------------------|
                   | ingestion.trips (py)  |-----> raw.green_tripdata
                   |                       |-----> raw.yellow_tripdata
                   | raw.payment_lookup    |-----> raw.payment_lookup
                   |   (csv seed)          |
                   +-----------------------+
                               |
                               v
                   +-----------------------+
                   |    STAGING LAYER      |
                   |-----------------------|
                   | staging.trips (sql)   |-----> staging.trips
                   |  - union green+yellow |
                   |  - dedup, clean       |
                   |  - join payment types |
                   +-----------------------+
                               |
                               v
                   +-----------------------+
                   |   REPORTING LAYER     |
                   |-----------------------|
                   | reports.trips_report  |-----> reports.trips_report
                   |   (sql)              |
                   |  - daily aggregations |
                   +-----------------------+
```

## DAG (Dependency Graph)

```
raw.payment_lookup ──┐
                     ├──> staging.trips ──> reports.trips_report
ingestion.trips ─────┘
```

All 4 assets are validated and executed in dependency order by Bruin.

---

## Project Structure

```
zoomcamp/
├── .bruin.yml                          # Bruin project config (connections, environments)
└── pipeline/
    ├── pipeline.yml                    # Pipeline definition (schedule, variables)
    ├── README.md                       # This file
    └── assets/
        ├── ingestion/
        │   ├── trips.py                # Python ingestion asset
        │   ├── payment_lookup.asset.yml# Seed asset definition
        │   ├── payment_lookup.csv      # Payment type reference data
        │   └── requirements.txt        # Python dependencies
        ├── staging/
        │   └── trips.sql               # Staging transformation
        └── reports/
            └── trips_report.sql        # Reporting aggregation
```

---

## Data Sources

| Source | Format | Location | Description |
|--------|--------|----------|-------------|
| NYC TLC Trip Data | Parquet | `https://d37ci6vzurychx.cloudfront.net/trip-data/` | Monthly trip records per taxi type |
| Payment Lookup | CSV | `assets/ingestion/payment_lookup.csv` | Static mapping of payment type IDs to names |

---

## Data Models

### Raw Layer (`raw` schema)

Raw data is loaded as-is with minimal transformations (column renaming only). This layer serves as the single source of truth for all downstream models.

#### `raw.green_tripdata`

Green taxi trip records. Loaded via `ingestion.trips`.

| Column | Type | Description |
|--------|------|-------------|
| `vendor_id` | text | LPEP provider (1=Creative Mobile, 2=VeriFone) |
| `service_type` | text | Always `"green"` |
| `pickup_datetime` | timestamp | Meter engaged timestamp |
| `dropoff_datetime` | timestamp | Meter disengaged timestamp |
| `store_and_fwd_flag` | text | Y/N - trip stored before sending to vendor |
| `rate_code_id` | float | Rate code (1=Standard, 2=JFK, 3=Newark, etc.) |
| `pickup_location_id` | float | TLC Taxi Zone pickup |
| `dropoff_location_id` | float | TLC Taxi Zone dropoff |
| `passenger_count` | float | Driver-entered passenger count |
| `trip_distance` | float | Trip distance in miles |
| `trip_type` | float | 1=Street-hail, 2=Dispatch |
| `fare_amount` | float | Time-and-distance fare |
| `extra` | float | Misc. extras and surcharges |
| `mta_tax` | float | MTA tax ($0.50) |
| `tip_amount` | float | Tip amount (cash tips not included) |
| `tolls_amount` | float | Total tolls paid |
| `ehail_fee` | float | E-hail fee (if applicable) |
| `improvement_surcharge` | float | $0.30 improvement surcharge |
| `congestion_surcharge` | float | Congestion surcharge |
| `airport_fee` | float | Airport fee (NULL for green) |
| `total_amount` | float | Total charged to passenger |
| `payment_type` | float | Payment method code |
| `extracted_at` | timestamp | UTC timestamp of extraction |

#### `raw.yellow_tripdata`

Yellow taxi trip records. Same schema as green with two differences:
- `trip_type` is always NULL (field does not exist in yellow taxi source)
- `airport_fee` is populated (e.g., $1.25 for JFK/LaGuardia pickups)

#### `raw.payment_lookup`

Static reference table seeded from CSV.

| Column | Type | PK | Description |
|--------|------|----|-------------|
| `payment_type_id` | integer | Yes | Payment method code |
| `payment_type_name` | varchar | | Human-readable name |

**Reference values:**

| ID | Name |
|----|------|
| 0 | flex_fare |
| 1 | credit_card |
| 2 | cash |
| 3 | no_charge |
| 4 | dispute |
| 5 | unknown |
| 6 | voided_trip |

---

### Staging Layer (`staging` schema)

Cleaned, deduplicated, and enriched data ready for analytics.

#### `staging.trips`

Union of green and yellow taxi trips with data quality filters applied.

**Materialization:** `table` (create+replace)

| Column | Type | PK | Description |
|--------|------|----|-------------|
| `trip_id` | varchar | Yes | Surrogate key: MD5 hash of `vendor_id + service_type + pickup_datetime + pickup_location_id + dropoff_location_id` |
| `vendor_id` | varchar | | LPEP/TPEP provider ID |
| `service_type` | varchar | | `"green"` or `"yellow"` |
| `pickup_datetime` | timestamp | | Meter engaged timestamp |
| `dropoff_datetime` | timestamp | | Meter disengaged timestamp |
| `trip_duration_minutes` | numeric | | Calculated: `(dropoff - pickup) / 60`, rounded to 2 decimals |
| `store_and_fwd_flag` | text | | Store and forward flag |
| `rate_code_id` | float | | Rate code |
| `pickup_location_id` | float | | TLC Taxi Zone pickup |
| `dropoff_location_id` | float | | TLC Taxi Zone dropoff |
| `passenger_count` | float | | Passenger count |
| `trip_distance` | float | | Distance in miles |
| `trip_type` | float | | Street-hail vs. dispatch (green only) |
| `fare_amount` | float | | Base fare |
| `extra` | float | | Extras and surcharges |
| `mta_tax` | float | | MTA tax |
| `tip_amount` | float | | Tip amount |
| `tolls_amount` | float | | Tolls |
| `ehail_fee` | float | | E-hail fee |
| `improvement_surcharge` | float | | Improvement surcharge |
| `congestion_surcharge` | float | | Congestion surcharge |
| `airport_fee` | float | | Airport fee (yellow only) |
| `total_amount` | float | | Total amount charged |
| `payment_type` | float | | Payment type code |
| `payment_type_name` | varchar | | Joined from `raw.payment_lookup` |
| `extracted_at` | timestamp | | Extraction timestamp |

**Transformations applied:**
- Union of `raw.green_tripdata` and `raw.yellow_tripdata`
- Deduplication via `ROW_NUMBER()` partitioned by `(vendor_id, service_type, pickup_datetime, pickup_location_id, dropoff_location_id)`, keeping the most recent `extracted_at`
- Surrogate key generation via `MD5` hash
- Trip duration calculation in minutes
- Left join to `raw.payment_lookup` for payment type name enrichment
- Rows filtered out: `vendor_id IS NULL`, `total_amount <= 0`, `trip_distance < 0`

---

### Reporting Layer (`reports` schema)

Pre-aggregated tables optimized for BI consumption.

#### `reports.trips_report`

Daily trip statistics aggregated by service type.

**Materialization:** `table` (create+replace)

| Column | Type | PK | Description |
|--------|------|----|-------------|
| `pickup_date` | date | Yes | Pickup date |
| `service_type` | varchar | Yes | `"green"` or `"yellow"` |
| `total_trips` | bigint | | Count of trips |
| `total_passengers` | float | | Sum of passengers |
| `avg_trip_distance_miles` | float | | Avg distance (miles) |
| `avg_trip_duration_minutes` | numeric | | Avg duration (minutes) |
| `total_fare_amount` | float | | Sum of base fares |
| `total_tip_amount` | float | | Sum of tips |
| `total_revenue` | float | | Sum of total amounts |
| `avg_revenue_per_trip` | float | | Avg revenue per trip |

---

## Data Quality Checks

Quality checks run automatically after each asset materializes.

### `raw.payment_lookup`

| Check | Column | Rule |
|-------|--------|------|
| not_null | `payment_type_id` | No NULL values |
| unique | `payment_type_id` | No duplicate IDs |
| not_null | `payment_type_name` | No NULL values |

### `staging.trips`

| Check | Column | Rule |
|-------|--------|------|
| not_null | `trip_id` | No NULL values |
| unique | `trip_id` | No duplicate surrogate keys |
| not_null | `service_type` | No NULL values |
| not_null | `pickup_datetime` | No NULL values |
| not_null | `total_amount` | No NULL values |
| non_negative | `total_amount` | `total_amount >= 0` |
| row_count_positive | (table) | At least 1 row exists |

### `reports.trips_report`

| Check | Column | Rule |
|-------|--------|------|
| not_null | `pickup_date` | No NULL values |
| not_null | `service_type` | No NULL values |
| not_null | `total_trips` | No NULL values |
| non_negative | `total_trips` | `total_trips >= 0` |
| non_negative | `total_passengers` | `total_passengers >= 0` |
| non_negative | `avg_trip_distance_miles` | Non-negative |
| non_negative | `avg_trip_duration_minutes` | Non-negative |
| non_negative | `total_fare_amount` | Non-negative |
| non_negative | `total_tip_amount` | Non-negative |
| not_null | `total_revenue` | No NULL values |
| non_negative | `total_revenue` | Non-negative |
| non_negative | `avg_revenue_per_trip` | Non-negative |

---

## Configuration

### Pipeline Variables

Defined in `pipeline.yml`:

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `taxi_types` | `array[string]` | `["green", "yellow"]` | Taxi types to ingest. Valid values: `"green"`, `"yellow"` |

### Connection

Defined in `.bruin.yml`:

| Parameter | Value |
|-----------|-------|
| Name | `pg-zoomcamp` |
| Host | `localhost` |
| Port | `5433` |
| Database | `nyc_taxi` |
| Schema | `public` |
| Pool max connections | `10` |

Environment variables override defaults in the Python ingestion asset:

| Env Var | Default | Description |
|---------|---------|-------------|
| `BRUIN_POSTGRES_USERNAME` | `root` | PostgreSQL username |
| `BRUIN_POSTGRES_PASSWORD` | `root` | PostgreSQL password |
| `BRUIN_POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `BRUIN_POSTGRES_PORT` | `5433` | PostgreSQL port |
| `BRUIN_POSTGRES_DATABASE` | `nyc_taxi` | PostgreSQL database name |

---

## Prerequisites

- [Bruin CLI](https://github.com/bruin-data/bruin) installed
- PostgreSQL running on `localhost:5433` with database `nyc_taxi`
- Python 3.9+ with dependencies:
  ```
  pandas==2.2.3
  pyarrow==17.0.0
  sqlalchemy==2.0.36
  psycopg2-binary==2.9.10
  ```

---

## Usage

### Validate the pipeline

```bash
bruin validate pipeline/pipeline.yml
```

### Run the full pipeline (specific date range)

```bash
bruin run \
  --start-date 2022-01-01T00:00:00.000Z \
  --end-date 2022-01-31T23:59:59.999999999Z \
  --environment default \
  pipeline/pipeline.yml
```

### Run a single asset

```bash
# Ingest trips only
bruin run \
  --start-date 2022-01-01T00:00:00.000Z \
  --end-date 2022-01-31T23:59:59.999999999Z \
  --environment default \
  pipeline/assets/ingestion/trips.py

# Run staging transformation only
bruin run \
  --start-date 2022-01-01T00:00:00.000Z \
  --end-date 2022-01-31T23:59:59.999999999Z \
  --environment default \
  pipeline/assets/staging/trips.sql
```

### Backfill multiple months

Run for each month sequentially, adjusting `--start-date` and `--end-date` accordingly. The ingestion is idempotent -- re-running deletes existing rows for the given window before reloading.

---

## Idempotency & Incremental Strategy

| Asset | Strategy | Detail |
|-------|----------|--------|
| `ingestion.trips` | Delete + append | Deletes rows matching the run window (`pickup_datetime` range), then appends new data. Safe to re-run. |
| `raw.payment_lookup` | Replace | Full replace on every run via dlt `replace` strategy. |
| `staging.trips` | Create + replace | Drops and recreates the table each run. |
| `reports.trips_report` | Create + replace | Drops and recreates the table each run. |

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `relation "raw.yellow_tripdata" does not exist` | `taxi_types` variable only includes `"green"` | Set `taxi_types` to `["green", "yellow"]` in `pipeline.yml` |
| `BRUIN_START_DATE and BRUIN_END_DATE must be set` | Missing `--start-date` / `--end-date` flags | Pass both flags to `bruin run` |
| Connection refused on port 5433 | PostgreSQL is not running | Start the database (e.g., `docker compose up -d`) |
| Slow ingestion for large date ranges | Downloading many months of Parquet files | Narrow the date window or run month-by-month |
| Duplicate rows in `staging.trips` | Should not happen -- dedup logic handles this | Check if `trip_id` unique check passes; inspect the dedup window columns |

---

## Data Lineage Summary

```
Source (NYC TLC Parquet) ──> raw.green_tripdata ──┐
                                                  ├──> staging.trips ──> reports.trips_report
Source (NYC TLC Parquet) ──> raw.yellow_tripdata ──┘         ^
                                                             |
                              raw.payment_lookup ────────────┘
```
