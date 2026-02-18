# NYC Taxi Data Platform - Data Engineering Documentation

| Field              | Value                                    |
|--------------------|------------------------------------------|
| **Project**        | Bruin NYC Taxi Data Platform             |
| **Owner**          | Data Engineering Team                    |
| **Last Updated**   | 2026-02-19                               |
| **Version**        | 1.0.0                                    |
| **Status**         | Production                               |
| **Review Cadence** | Quarterly                                |

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Bruin + dbt Integration Pattern](#2-bruin--dbt-integration-pattern)
3. [Architecture](#3-architecture)
4. [Infrastructure](#4-infrastructure)
5. [Data Flow & Pipeline Design](#5-data-flow--pipeline-design)
6. [Data Sources](#6-data-sources)
7. [Data Model & Schema Design](#7-data-model--schema-design)
8. [Data Dictionary](#8-data-dictionary)
9. [Configuration Reference](#9-configuration-reference)
10. [Deployment & Release Process](#10-deployment--release-process)
11. [Operational Runbook](#11-operational-runbook)
12. [Monitoring, Alerting & SLAs](#12-monitoring-alerting--slas)
13. [Data Quality Framework](#13-data-quality-framework)
14. [Disaster Recovery & Backups](#14-disaster-recovery--backups)
15. [Access Control & Security](#15-access-control--security)
16. [Troubleshooting Guide](#16-troubleshooting-guide)
17. [Change Log](#17-change-log)

---

## 1. System Overview

### 1.1 Purpose

This platform ingests, transforms, and serves NYC Taxi & Limousine Commission (TLC) trip data for analytics and business intelligence. It is built on an **integrated Bruin + dbt architecture** — Bruin serves as the unified orchestration layer that coordinates Python-based data ingestion and dbt-powered SQL transformations within a single pipeline definition.

It provides a clean, modeled, and reliable dataset that enables:

- Revenue analysis by zone, time period, and service type
- Operational metrics (trip duration, distance, passenger count)
- Vendor performance tracking
- Demand pattern analysis across boroughs

### 1.2 Scope

| In Scope                                       | Out of Scope                          |
|------------------------------------------------|---------------------------------------|
| Green & Yellow taxi trip ingestion             | For-Hire Vehicle (FHV) data           |
| Bruin-orchestrated end-to-end pipeline         | Real-time streaming ingestion         |
| dbt-powered staging, intermediate, and marts   | Machine learning / forecasting models |
| Monthly revenue aggregations                   | External API serving layer            |
| Data quality checks (freshness, schema tests)  | Multi-cloud deployment                |
| Local development environment (Docker)         |                                       |

### 1.3 Technology Stack

| Layer            | Technology                | Version   | Role in Pipeline                                              |
|------------------|---------------------------|-----------|---------------------------------------------------------------|
| **Orchestration**| **Bruin**                 | Latest    | Pipeline definition, asset scheduling, dependency management, variable passing |
| **Transformation** | **dbt-core**            | >= 1.0    | SQL-based data modeling, testing, documentation, lineage      |
| Database         | PostgreSQL                | 15-alpine | Analytical data warehouse                                     |
| Containerization | Docker / Docker Compose   | Latest    | Infrastructure as Code                                        |
| DB Admin         | pgAdmin 4                 | Latest    | Database management UI                                        |
| Language         | Python 3.x               | >= 3.9    | Ingestion scripts (Bruin Python assets)                       |
| Data Format      | Apache Parquet            | -         | Source file format                                            |

### 1.4 Key Stakeholders

| Role                  | Responsibility                                              |
|-----------------------|-------------------------------------------------------------|
| Data Engineer         | Bruin pipeline development, infrastructure, ingestion       |
| Analytics Engineer    | dbt model design, business logic, testing, documentation    |
| BI Analyst            | Dashboard development, ad-hoc queries on mart tables        |
| Data Platform Lead    | Architecture decisions, SLA ownership, incident escalation  |

---

## 2. Bruin + dbt Integration Pattern

### 2.1 Why Bruin + dbt?

This project uses a **dual-tool architecture** where each tool handles what it does best:

| Concern              | Tool      | Why This Tool                                                |
|----------------------|-----------|--------------------------------------------------------------|
| **Orchestration**    | **Bruin** | Unified pipeline definition, cross-language asset support (Python + SQL + dbt), parameterized runs, connection management, lineage across ingestion and transformation |
| **Transformation**   | **dbt**   | SQL-first modeling, incremental materializations, built-in testing framework, auto-generated documentation, rich package ecosystem |

dbt alone cannot ingest data from external sources or orchestrate non-SQL workloads. Bruin alone does not provide dbt's mature modeling framework (ref, source, incremental, testing). Together, they form a complete data platform:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    BRUIN (Orchestrator)                              │
│   "What runs, when, in what order, with what parameters"            │
│                                                                     │
│   pipeline.yml defines:                                             │
│   ├── Connections (shared across all assets)                        │
│   ├── Variables (year, month, taxi_type — passed to all assets)     │
│   ├── Asset 1: Python ingestion script  ← Bruin executes directly  │
│   └── Asset 2: dbt project             ← Bruin delegates to dbt    │
│                                                                     │
│   Bruin handles:                                                    │
│   • Dependency ordering (ingestion runs before transformation)      │
│   • Connection injection (database credentials via env vars)        │
│   • Variable passing (BRUIN_VARS → Python, dbt vars)                │
│   • Cross-asset lineage (Python → dbt in one DAG)                   │
│   • Validation (bruin validate checks all assets)                   │
├─────────────────────────────────────────────────────────────────────┤
│                    dbt (Transformation Engine)                       │
│   "How raw data becomes analytics-ready tables"                     │
│                                                                     │
│   ny_taxi_dbt_core/ project handles:                                │
│   ├── Source definitions (raw.green_tripdata, raw.yellow_tripdata)  │
│   ├── Staging models (rename, cast, filter)                         │
│   ├── Intermediate models (union, deduplicate, enrich)              │
│   ├── Mart models (star schema: facts + dimensions)                 │
│   ├── Seed data (payment_type_lookup, taxi_zone_lookup)             │
│   ├── Tests (not_null, unique, accepted_values, relationships)      │
│   ├── Macros (get_trip_duration_minutes, get_vendor_data)           │
│   └── Documentation (schema.yml, sources.yml)                       │
│                                                                     │
│   dbt handles:                                                      │
│   • Model dependency resolution (ref/source graph)                  │
│   • Materialization strategy (view, table, incremental)             │
│   • Data quality testing (built-in + dbt_expectations)              │
│   • Source freshness monitoring                                     │
│   • Auto-generated data documentation & lineage                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 How Bruin Invokes dbt

Bruin treats the dbt project as a **first-class asset** in the pipeline. The integration works through two mechanisms:

**Mechanism 1: Native dbt Asset Type (pipeline.yml)**
```yaml
# pipeline.yml
assets:
  - name: dbt_models
    type: dbt
    path: assets/ny_taxi_dbt_core    # Points to the dbt project root
    connection: postgres_local        # Bruin-managed connection
```

**Mechanism 2: Python Wrapper Asset (dbt_models.py)**
```python
# assets/dbt_models.py — Bruin Python asset that shells out to dbt CLI
def main():
    dbt_path = Path(__file__).resolve().parents[1] / "assets" / "ny_taxi_dbt_core"
    subprocess.run(["dbt", "deps"], cwd=str(dbt_path), check=True)
    subprocess.run(["dbt", "build"], cwd=str(dbt_path), check=True)
```

This dual approach provides flexibility:
- The **native `type: dbt`** declaration gives Bruin visibility into the dbt DAG for lineage and validation
- The **Python wrapper** ensures `dbt deps` runs before `dbt build` and provides custom error handling

### 2.3 Separation of Concerns

```
┌─────────────────────────────────────────────────────────┐
│                 Bruin Owns                               │
├─────────────────────────────────────────────────────────┤
│ • Pipeline-level configuration (.bruin.yml)             │
│ • Asset execution order & dependencies (pipeline.yml)   │
│ • Database connection management (connection pooling)   │
│ • Runtime variables (year, month, taxi_type)            │
│ • Cross-asset lineage (Python → dbt assets)             │
│ • Pipeline validation (bruin validate)                  │
│ • Execution entry point (bruin run)                     │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                  dbt Owns                                │
├─────────────────────────────────────────────────────────┤
│ • SQL model definitions (staging → intermediate → mart) │
│ • Model materialization strategy (view/table/incr.)     │
│ • Data transformation logic (all SQL business rules)    │
│ • Data quality tests (schema + data tests)              │
│ • Source freshness checks                               │
│ • Seed data management (CSV → tables)                   │
│ • Reusable macros (Jinja SQL templates)                 │
│ • Model documentation (schema.yml, descriptions)        │
│ • Package management (dbt_utils, dbt_expectations)      │
└─────────────────────────────────────────────────────────┘
```

### 2.4 Data Contract Between Bruin Ingestion and dbt

The handoff point between Bruin's Python ingestion and dbt's transformation layer is the `raw` schema. This acts as a **data contract**:

| Contract Term               | Specification                                              |
|-----------------------------|------------------------------------------------------------|
| **Schema**                  | `raw`                                                      |
| **Tables**                  | `green_tripdata`, `yellow_tripdata`                        |
| **Required columns (green)**| `VendorID`, `lpep_pickup_datetime`, `lpep_dropoff_datetime`, `PULocationID`, `DOLocationID`, `passenger_count`, `trip_distance`, `fare_amount`, `total_amount`, `payment_type`, etc. |
| **Required columns (yellow)**| `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`, `DOLocationID`, `passenger_count`, `trip_distance`, `fare_amount`, `total_amount`, `payment_type`, etc. |
| **Metadata columns**        | `ingestion_year` (INT), `ingestion_month` (INT)            |
| **Freshness**               | Loaded within 48 hours (enforced by dbt source freshness)  |
| **Idempotency**             | Ingestion guarantees no duplicates per (year, month)       |

If the ingestion script changes the raw table schema, the dbt staging models (`stg_green_tripdata`, `stg_yellow_tripdata`) must be updated accordingly. This contract is enforced by:
1. **dbt source definitions** (`sources.yml`) — declares expected columns
2. **dbt source freshness** — alerts when data is stale
3. **dbt staging tests** — catches schema mismatches at build time

### 2.5 Configuration Mapping

Both tools need database connection details. Here's how they stay in sync:

| Setting          | Bruin (`.bruin.yml`)         | dbt (`profiles.yml`)        | Notes                          |
|------------------|-------------------------------|-----------------------------|--------------------------------|
| Host             | `host: localhost`             | `host: localhost`           | Must match                     |
| Port             | `port: 5432`                  | `port: 5432`               | Must match                     |
| Database         | `database: nyc_taxi`          | `database: nyc_taxi`       | Must match                     |
| Username         | `username: root`              | `user: root`               | Different key names            |
| Password         | `password: root`              | `password: root`           | Must match                     |
| Default Schema   | `schema: raw`                 | `schema: dev` / `prod`     | Intentionally different — Bruin writes to `raw`, dbt reads from `raw` and writes to `dev`/`prod` |

> **Production Recommendation**: Use environment variables or a secrets manager so both tools read from a single source of truth for credentials, eliminating drift risk.

---

## 3. Architecture

### 3.1 High-Level Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL DATA SOURCE                             │
│              NYC TLC Parquet Files (d37ci6vzurychx.cloudfront.net)       │
└──────────────────────────┬───────────────────────────────────────────────┘
                           │  HTTPS / Parquet
                           ▼
┌══════════════════════════════════════════════════════════════════════════┐
║                  BRUIN — ORCHESTRATION LAYER                            ║
║           pipeline.yml  |  .bruin.yml  |  bruin run                     ║
║  Manages: execution order, connections, variables, cross-asset lineage  ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  ┌─ ASSET 1: Bruin Python Asset ─────────────────────────────────────┐  ║
║  │  ingest_taxi_data.py                                              │  ║
║  │  Bruin injects: BRUIN_VARS (year, month, taxi_type)               │  ║
║  │  Bruin injects: BRUIN_POSTGRES_* (connection credentials)         │  ║
║  │                                                                    │  ║
║  │  ┌──────────────────────┐   ┌──────────────────────┐              │  ║
║  │  │ Download green.parquet│   │ Download yellow.parquet│             │  ║
║  │  └──────────┬───────────┘   └──────────┬───────────┘              │  ║
║  │             ▼                           ▼                          │  ║
║  │  ┌──────────────────┐       ┌──────────────────────┐              │  ║
║  │  │ raw.green_tripdata│      │ raw.yellow_tripdata   │             │  ║
║  │  └──────────────────┘       └──────────────────────┘              │  ║
║  └────────────────────────────────────────────────────────────────────┘  ║
║                           │                                              ║
║           DATA CONTRACT: raw schema (handoff point)                      ║
║                           │                                              ║
║  ┌─ ASSET 2: Bruin dbt Asset ────────────────────────────────────────┐  ║
║  │  type: dbt  |  path: assets/ny_taxi_dbt_core                      │  ║
║  │  Bruin delegates execution to dbt CLI (dbt deps → dbt build)      │  ║
║  │                                                                    │  ║
║  │  ┌────────────────────────────────────────────────────────────┐   │  ║
║  │  │              dbt — TRANSFORMATION ENGINE                    │   │  ║
║  │  │   Manages: SQL models, tests, docs, seeds, macros          │   │  ║
║  │  │                                                             │   │  ║
║  │  │  STAGING (views)                                            │   │  ║
║  │  │  ┌─────────────────────┐  ┌──────────────────────┐         │   │  ║
║  │  │  │ stg_green_tripdata  │  │ stg_yellow_tripdata  │         │   │  ║
║  │  │  └─────────┬───────────┘  └──────────┬───────────┘         │   │  ║
║  │  │            │                          │                     │   │  ║
║  │  │  INTERMEDIATE (tables)                │                     │   │  ║
║  │  │  ┌─────────▼──────────────────────────▼──┐                  │   │  ║
║  │  │  │       int_trips_unioned               │                  │   │  ║
║  │  │  └─────────────────┬─────────────────────┘                  │   │  ║
║  │  │                    ▼                                        │   │  ║
║  │  │  ┌─────────────────────────────┐  ┌──────────────────────┐  │   │  ║
║  │  │  │  int_trips                  │  │  payment_type_lookup │  │   │  ║
║  │  │  │  (deduplicated + enriched)  │◄─┤  (dbt seed)         │  │   │  ║
║  │  │  └─────────────────┬───────────┘  └──────────────────────┘  │   │  ║
║  │  │                    │                                        │   │  ║
║  │  │  MARTS (tables)    ▼                                        │   │  ║
║  │  │  ┌─────────────────────────────┐  ┌──────────────────────┐  │   │  ║
║  │  │  │  fact_trips (incremental)   │◄─┤  dim_zones           │  │   │  ║
║  │  │  │  (star schema fact table)   │  │  (dbt seed)          │  │   │  ║
║  │  │  └──────────┬──────────────────┘  └──────────────────────┘  │   │  ║
║  │  │             │                                               │   │  ║
║  │  │             │  ┌────────────────────────────────────┐       │   │  ║
║  │  │             ├─►│  dim_vendors                       │       │   │  ║
║  │  │             │  └────────────────────────────────────┘       │   │  ║
║  │  │             │  ┌────────────────────────────────────┐       │   │  ║
║  │  │             └─►│  fact_monthly_zone_revenue          │      │   │  ║
║  │  │                │  (aggregated reporting table)       │      │   │  ║
║  │  │                └────────────────────────────────────┘       │   │  ║
║  │  └────────────────────────────────────────────────────────────┘   │  ║
║  └────────────────────────────────────────────────────────────────────┘  ║
╚══════════════════════════════════════════════════════════════════════════╝
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                     CONSUMPTION LAYER                                     │
│         BI Tools / Dashboards / Ad-hoc SQL via pgAdmin                   │
└──────────────────────────────────────────────────────────────────────────┘
```

> **Key Insight**: The outer boundary (double lines `║`) is Bruin's orchestration scope. The inner boundary is dbt's transformation scope. Bruin owns the *when* and *what order*; dbt owns the *how* of data transformation.

### 3.2 Data Modeling Approach

This project follows the **Medallion Architecture** pattern. Each layer is owned by a specific tool in the Bruin + dbt stack:

| Layer          | Convention       | Materialization | Owned By       | Purpose                                 |
|----------------|------------------|-----------------|----------------|-----------------------------------------|
| Bronze / Raw   | `raw.*`          | Table (pandas)  | **Bruin** (Python asset) | Landed source data, no transformation   |
| Silver / STG   | `stg_*`          | View            | **dbt** (staging models) | Renamed, typed, filtered                |
| Silver / INT   | `int_*`          | Table           | **dbt** (intermediate)   | Unioned, deduplicated, enriched         |
| Gold / Marts   | `fact_*`, `dim_*`| Table / Incr.   | **dbt** (mart models)    | Star schema for analytics consumption   |

### 3.3 Star Schema Design

```
                    ┌──────────────┐
                    │  dim_vendors │
                    │──────────────│
                    │ vendor_id PK │
                    │ vendor_name  │
                    └──────┬───────┘
                           │
┌──────────────┐   ┌──────┴────────────────────────────┐   ┌──────────────┐
│  dim_zones   │   │          fact_trips                │   │  dim_zones   │
│──────────────│   │───────────────────────────────────│   │──────────────│
│location_id PK│◄──│ pickup_location_id  FK            │   │location_id PK│
│ borough      │   │ dropoff_location_id FK ───────────│──►│ borough      │
│ zone         │   │ vendor_id           FK            │   │ zone         │
│ service_zone │   │ trip_id             PK            │   │ service_zone │
└──────────────┘   │ service_type                      │   └──────────────┘
                   │ pickup_datetime                    │
                   │ dropoff_datetime                   │
                   │ trip_duration_minutes              │
                   │ passenger_count                    │
                   │ trip_distance                      │
                   │ fare_amount                        │
                   │ total_amount                       │
                   │ payment_type_description           │
                   └───────────────────────────────────┘
```

---

## 4. Infrastructure

### 4.1 Docker Services

| Service    | Image                  | Port  | Purpose                       |
|------------|------------------------|-------|-------------------------------|
| postgres   | `postgres:15-alpine`   | 5432  | Primary data warehouse        |
| pgadmin    | `dpage/pgadmin4:latest`| 5050  | Database administration UI    |

### 4.2 Infrastructure Setup

```bash
# 1. Start infrastructure
cd 05-data-platforms
docker compose up -d

# 2. Verify services are healthy
docker compose ps

# 3. Access pgAdmin
#    URL:      http://localhost:5050
#    Email:    admin@localhost.com
#    Password: admin
```

### 4.3 Database Schemas

| Schema | Owner | Purpose                                      |
|--------|-------|----------------------------------------------|
| `raw`  | root  | Landed source data from ingestion scripts     |
| `dev`  | root  | dbt development environment models            |
| `prod` | root  | dbt production environment models             |

### 4.4 Resource Requirements

| Resource | Minimum  | Recommended | Notes                              |
|----------|----------|-------------|------------------------------------|
| CPU      | 2 cores  | 4 cores     | dbt build is CPU-bound             |
| RAM      | 4 GB     | 8 GB        | Parquet reads are memory-intensive  |
| Disk     | 10 GB    | 50 GB       | Scales with number of months loaded |
| Network  | 10 Mbps  | 50 Mbps     | Parquet downloads ~50-200 MB each   |

---

## 5. Data Flow & Pipeline Design

### 5.1 Pipeline Overview

```
pipeline.yml
├── Asset 1: ingest_taxi_data.py   (type: python)
│   ├── Downloads parquet from NYC TLC CDN
│   ├── Creates raw schema if not exists
│   ├── Deduplicates by (year, month) partition
│   └── Appends to raw.{green|yellow}_tripdata
│
└── Asset 2: dbt_models            (type: dbt)
    ├── dbt deps  (install packages)
    └── dbt build (seeds + models + tests)
        ├── Seeds: payment_type_lookup, taxi_zone_lookup
        ├── Staging: stg_green_tripdata, stg_yellow_tripdata
        ├── Intermediate: int_trips_unioned → int_trips
        └── Marts: dim_zones, dim_vendors, fact_trips, fact_monthly_zone_revenue
```

### 5.2 Pipeline Parameters

| Parameter   | Type    | Default | Valid Values       | Description                      |
|-------------|---------|---------|--------------------|---------------------------------|
| `year`      | integer | 2019    | 2009-2024          | Trip data year                   |
| `month`     | integer | 1       | 1-12               | Trip data month                  |
| `taxi_type` | string  | green   | `green`, `yellow`  | Taxi service type to ingest      |

### 5.3 Execution Commands

```bash
# Validate pipeline configuration
bruin validate

# Full pipeline run (ingestion + transformation)
bruin run

# Ingestion only - with default parameters
bruin run assets/ingestion/ingest_taxi_data.py

# Ingestion only - green taxi, specific month
bruin run assets/ingestion/ingest_taxi_data.py \
  --var '{"year": 2019, "month": 3, "taxi_type": "green"}'

# Ingestion only - yellow taxi
bruin run assets/ingestion/ingest_taxi_data.py \
  --var '{"year": 2019, "month": 1, "taxi_type": "yellow"}'

# Transformation only (dbt models)
bruin run dbt_models

# View data lineage
bruin lineage assets/ingestion/ingest_taxi_data.py
```

### 5.4 Ingestion Idempotency

The ingestion script implements **delete-then-insert** idempotency:

1. Check if rows exist for the target `(year, month)` partition
2. If rows exist, delete them
3. Insert new data via chunked append (`chunksize=10000`)

This ensures re-running ingestion for the same period does not produce duplicates.

### 5.5 Incremental Loading Strategy

The `fact_trips` mart uses dbt's **incremental materialization** with merge strategy:

```yaml
materialized: incremental
unique_key: trip_id
incremental_strategy: merge
on_schema_change: append_new_columns
```

- **First run**: Full table build
- **Subsequent runs**: Only new/changed `trip_id` records are merged
- **Schema changes**: New columns are appended automatically

---

## 6. Data Sources

### 6.1 NYC TLC Trip Record Data

| Attribute        | Detail                                                         |
|------------------|----------------------------------------------------------------|
| **Provider**     | NYC Taxi & Limousine Commission                                |
| **URL Pattern**  | `https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_tripdata_{year}-{month}.parquet` |
| **Format**       | Apache Parquet                                                 |
| **Update Freq.** | Monthly (published ~2 months after trip month)                 |
| **Coverage**     | 2009 - present                                                 |
| **License**      | Public domain                                                  |
| **Size**         | ~50-200 MB per month per taxi type                             |

### 6.2 Static Reference Data (Seeds)

| Seed File               | Rows | Update Frequency | Description                              |
|--------------------------|------|------------------|------------------------------------------|
| `taxi_zone_lookup.csv`   | 265  | Rarely           | TLC zone ID → borough, zone, service_zone |
| `payment_type_lookup.csv`| 7    | Rarely           | Payment type code → description          |

### 6.3 Source Freshness SLA

Configured in `models/staging/sources.yml`:

| Threshold | Period   | Action           |
|-----------|----------|------------------|
| Warning   | 24 hours | Log warning      |
| Error     | 48 hours | Fail pipeline    |

---

## 7. Data Model & Schema Design

### 7.1 Model Dependency Graph (DAG)

```
raw.green_tripdata ──► stg_green_tripdata ──┐
                                             ├──► int_trips_unioned ──► int_trips ──► fact_trips
raw.yellow_tripdata ─► stg_yellow_tripdata ──┘          │                    │            │
                                                         │                    │            ├──► dim_vendors
                                             payment_type_lookup ─────────────┘            │
                                                                                           ├──► fact_monthly_zone_revenue
                                             taxi_zone_lookup ──► dim_zones ───────────────┘
```

### 7.2 Model Inventory

| Model                         | Layer        | Materialized | Grain                        | Row Estimate |
|-------------------------------|--------------|--------------|------------------------------|--------------|
| `stg_green_tripdata`          | Staging      | View         | One row per trip             | ~600K/month  |
| `stg_yellow_tripdata`         | Staging      | View         | One row per trip             | ~7M/month    |
| `int_trips_unioned`           | Intermediate | Table        | One row per trip             | ~7.6M/month  |
| `int_trips`                   | Intermediate | Table        | One row per trip (deduped)   | ~7.6M/month  |
| `fact_trips`                  | Mart         | Incremental  | One row per trip             | ~7.6M/month  |
| `dim_zones`                   | Mart         | Table        | One row per zone             | 265          |
| `dim_vendors`                 | Mart         | Table        | One row per vendor           | ~3           |
| `fact_monthly_zone_revenue`   | Mart         | Table        | Zone x Month x Service Type  | ~800/month   |

### 7.3 Transformation Logic Summary

| Model                  | Key Transformations                                                        |
|------------------------|---------------------------------------------------------------------------|
| `stg_green_tripdata`   | CamelCase → snake_case rename, type casting, NULL vendor_id filter         |
| `stg_yellow_tripdata`  | CamelCase → snake_case rename, type casting, NULL vendor_id filter         |
| `int_trips_unioned`    | UNION ALL green+yellow, add `service_type` flag, normalize missing fields  |
| `int_trips`            | Surrogate key (`trip_id`), payment type enrichment, deduplication via ROW_NUMBER |
| `fact_trips`           | Zone enrichment (borough/zone), trip duration calculation, type precision   |
| `dim_zones`            | Direct mapping from seed data                                              |
| `dim_vendors`          | Distinct vendor IDs with macro-based name mapping                          |
| `fact_monthly_zone_revenue` | GROUP BY (zone, month, service_type), SUM revenue, AVG metrics       |

---

## 8. Data Dictionary

### 8.1 fact_trips

| Column                     | Type          | Nullable | Description                                          |
|----------------------------|---------------|----------|------------------------------------------------------|
| `trip_id`                  | TEXT          | No       | Surrogate key (vendor_id + pickup_datetime + location + service_type) |
| `vendor_id`                | INTEGER       | No       | 1 = Creative Mobile Technologies, 2 = VeriFone Inc.  |
| `service_type`             | TEXT          | No       | `Green` or `Yellow`                                  |
| `rate_code_id`             | INTEGER       | Yes      | 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group |
| `pickup_location_id`       | INTEGER       | Yes      | FK → dim_zones.location_id                           |
| `pickup_borough`           | TEXT          | Yes      | Denormalized borough name                            |
| `pickup_zone`              | TEXT          | Yes      | Denormalized zone name                               |
| `dropoff_location_id`      | INTEGER       | Yes      | FK → dim_zones.location_id                           |
| `dropoff_borough`          | TEXT          | Yes      | Denormalized borough name                            |
| `dropoff_zone`             | TEXT          | Yes      | Denormalized zone name                               |
| `pickup_datetime`          | TIMESTAMP     | No       | Trip start time                                      |
| `dropoff_datetime`         | TIMESTAMP     | No       | Trip end time                                        |
| `store_and_fwd_flag`       | TEXT          | Yes      | Y = store-and-forward trip, N = not                  |
| `passenger_count`          | INTEGER       | Yes      | Number of passengers (driver-entered)                |
| `trip_distance`            | NUMERIC(10,2) | Yes      | Trip distance in miles                               |
| `trip_type`                | INTEGER       | Yes      | 1 = Street-hail, 2 = Dispatch                       |
| `trip_duration_minutes`    | BIGINT        | Yes      | Calculated: dropoff - pickup in minutes              |
| `fare_amount`              | NUMERIC(18,2) | Yes      | Metered fare amount in USD                           |
| `extra`                    | NUMERIC(18,2) | Yes      | Rush hour and overnight surcharges                   |
| `mta_tax`                  | NUMERIC(18,2) | Yes      | MTA tax ($0.50)                                      |
| `tip_amount`               | NUMERIC(18,2) | Yes      | Tip amount (auto-populated for credit card)          |
| `tolls_amount`             | NUMERIC(18,2) | Yes      | Bridge and tunnel tolls                              |
| `ehail_fee`                | NUMERIC(18,2) | Yes      | E-hail fee (green taxis only)                        |
| `improvement_surcharge`    | NUMERIC(18,2) | Yes      | $0.30 improvement surcharge                          |
| `total_amount`             | NUMERIC(18,2) | Yes      | Total charged to passenger                           |
| `payment_type`             | INTEGER       | Yes      | 1=Credit, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown |
| `payment_type_description` | TEXT          | Yes      | Human-readable payment type                          |

### 8.2 dim_zones

| Column         | Type    | Nullable | Description                                        |
|----------------|---------|----------|----------------------------------------------------|
| `location_id`  | INTEGER | No       | Primary key (TLC zone ID, 1-265)                   |
| `borough`      | TEXT    | No       | Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR, Unknown |
| `zone`         | TEXT    | No       | Specific neighborhood/area name                    |
| `service_zone` | TEXT    | Yes      | Yellow Zone, Boro Zone, Airports, EWR, N/A         |

### 8.3 dim_vendors

| Column        | Type    | Nullable | Description                       |
|---------------|---------|----------|-----------------------------------|
| `vendor_id`   | INTEGER | No       | Primary key                       |
| `vendor_name` | TEXT    | Yes      | Creative Mobile Technologies, VeriFone Inc., Unknown/Other |

### 8.4 fact_monthly_zone_revenue

| Column                                  | Type          | Nullable | Description                        |
|-----------------------------------------|---------------|----------|------------------------------------|
| `pickup_zone`                           | TEXT          | No       | Zone name (or 'Unknown Zone')      |
| `revenue_month`                         | TIMESTAMP     | No       | Truncated to first of month        |
| `service_type`                          | TEXT          | No       | Green or Yellow                    |
| `revenue_monthly_fare`                  | NUMERIC       | Yes      | Sum of fare amounts                |
| `revenue_monthly_extra`                 | NUMERIC       | Yes      | Sum of extras                      |
| `revenue_monthly_mta_tax`              | NUMERIC       | Yes      | Sum of MTA tax                     |
| `revenue_monthly_tip_amount`           | NUMERIC       | Yes      | Sum of tips                        |
| `revenue_monthly_tolls_amount`         | NUMERIC       | Yes      | Sum of tolls                       |
| `revenue_monthly_ehail_fee`            | NUMERIC       | Yes      | Sum of e-hail fees                 |
| `revenue_monthly_improvement_surcharge`| NUMERIC       | Yes      | Sum of improvement surcharges      |
| `revenue_monthly_total_amount`         | NUMERIC       | Yes      | Sum of total amounts               |
| `total_monthly_trips`                  | BIGINT        | No       | Count of trips                     |
| `avg_monthly_passenger_count`          | NUMERIC(10,2) | Yes      | Average passengers per trip        |
| `avg_monthly_trip_distance`            | NUMERIC(10,2) | Yes      | Average trip distance (miles)      |

---

## 9. Configuration Reference

### 9.1 Bruin Configuration (`.bruin.yml`)

```yaml
default_environment: default
environments:
  default:
    connections:
      postgres:
        - name: "postgres_local"
          host: "localhost"
          port: 5432
          database: "nyc_taxi"
          username: "root"           # Use secrets manager in production
          password: "root"           # Use secrets manager in production
          schema: "raw"
          pool_max_conns: 10
```

> **Production Note**: Replace hardcoded credentials with environment variables or a secrets manager (e.g., AWS Secrets Manager, HashiCorp Vault).

### 9.2 Pipeline Configuration (`pipeline.yml`)

```yaml
name: bruin-nyc-taxi
default_connections:
  postgres: postgres_local
variables:
  year:
    type: integer
    default: 2019
  month:
    type: integer
    default: 1
  taxi_type:
    type: string
    default: green
```

### 9.3 dbt Profile Configuration (`profiles.yml`)

| Target | Schema | Threads | Use Case                     |
|--------|--------|---------|------------------------------|
| `dev`  | `dev`  | 4       | Development & testing        |
| `prod` | `prod` | 4       | Production workloads         |

### 9.4 dbt Packages

| Package                        | Version  | Purpose                                  |
|--------------------------------|----------|------------------------------------------|
| `dbt-labs/dbt_utils`           | 1.3.3    | `generate_surrogate_key`, general utils  |
| `dbt-labs/codegen`             | 0.14.0   | Code generation helpers                  |
| `dbt-labs/audit_helper`        | 0.12.2   | Data audit & comparison                  |
| `metaplane/dbt_expectations`   | 0.10.10  | Great Expectations-style tests           |
| `godatadriven/dbt_date`        | 0.17.1   | Date utility functions                   |

---

## 10. Deployment & Release Process

### 10.1 Environment Promotion

```
┌──────────┐    PR + Review     ┌──────────┐    Tag Release    ┌──────────┐
│   Dev    │ ─────────────────► │  Staging │ ────────────────► │   Prod   │
│ (local)  │                    │ (CI/CD)  │                   │ (target) │
└──────────┘                    └──────────┘                   └──────────┘
```

### 10.2 Development Workflow

```bash
# 1. Start local infrastructure
docker compose up -d

# 2. Run ingestion for test data
bruin run assets/ingestion/ingest_taxi_data.py \
  --var '{"year": 2019, "month": 1, "taxi_type": "green"}'

# 3. Run dbt in dev mode
cd assets/ny_taxi_dbt_core
dbt deps
dbt build --target dev

# 4. Validate with tests
dbt test --target dev

# 5. Generate and review documentation
dbt docs generate
dbt docs serve
```

### 10.3 Production Deployment Checklist

- [ ] All dbt tests pass on `dev` target
- [ ] `bruin validate` succeeds
- [ ] Source freshness checks pass (`dbt source freshness`)
- [ ] Code reviewed and approved via PR
- [ ] No breaking schema changes (or migration plan documented)
- [ ] dbt build succeeds on `prod` target
- [ ] Row count validation post-deployment
- [ ] Stakeholders notified of any schema changes

### 10.4 Rollback Procedure

1. **dbt models**: Re-run the previous dbt version with `dbt build --full-refresh --target prod`
2. **Ingestion**: Re-run ingestion for the affected partition (idempotent by design)
3. **Infrastructure**: `docker compose down && docker compose up -d`

---

## 11. Operational Runbook

### 11.1 Daily Operations

| Time  | Task                                        | Command                                           |
|-------|---------------------------------------------|----------------------------------------------------|
| 06:00 | Ingest latest available month (green)       | `bruin run assets/ingestion/ingest_taxi_data.py --var '{"taxi_type":"green"}'` |
| 06:30 | Ingest latest available month (yellow)      | `bruin run assets/ingestion/ingest_taxi_data.py --var '{"taxi_type":"yellow"}'` |
| 07:00 | Run dbt transformations                     | `bruin run dbt_models`                             |
| 07:30 | Verify source freshness                     | `dbt source freshness --target prod`               |

### 11.2 Monthly Operations

| Task                               | Description                                              |
|------------------------------------|----------------------------------------------------------|
| Backfill check                     | Verify no gaps in ingested months                        |
| Seed data review                   | Check for TLC zone/payment type updates                  |
| dbt package updates                | Review and update dbt package versions                   |
| Disk usage monitoring              | Check PostgreSQL storage consumption                     |
| Performance review                 | Review query times on mart tables, add indexes if needed |

### 11.3 Common Operational Commands

```bash
# Check pipeline status
bruin validate

# Check database connectivity
docker exec nyc_transport_postgres pg_isready -U root -d nyc_taxi

# Check table row counts
docker exec nyc_transport_postgres psql -U root -d nyc_taxi -c "
  SELECT schemaname, tablename, n_live_tup
  FROM pg_stat_user_tables
  ORDER BY schemaname, tablename;"

# Check for data gaps
docker exec nyc_transport_postgres psql -U root -d nyc_taxi -c "
  SELECT ingestion_year, ingestion_month, count(*)
  FROM raw.green_tripdata
  GROUP BY 1, 2
  ORDER BY 1, 2;"

# Full refresh of all models
cd assets/ny_taxi_dbt_core && dbt build --full-refresh --target prod

# Rebuild a single model and its dependents
dbt build --select fact_trips+ --target prod
```

---

## 12. Monitoring, Alerting & SLAs

### 12.1 Service Level Agreements

| SLA                         | Target     | Measurement                                    |
|-----------------------------|------------|-------------------------------------------------|
| Data freshness              | < 48 hours | Time since last ingestion run                   |
| Pipeline completion time    | < 60 min   | End-to-end wall clock time                      |
| Data availability (uptime)  | 99.5%      | Mart tables queryable                           |
| Data accuracy               | > 99.9%    | dbt test pass rate                              |

### 12.2 Key Metrics to Monitor

| Metric                    | Warning Threshold | Critical Threshold | Check Method                     |
|---------------------------|-------------------|--------------------|----------------------------------|
| Source freshness           | > 24 hours        | > 48 hours         | `dbt source freshness`           |
| Row count delta            | > 20% change      | > 50% change       | Compare with previous month      |
| dbt test failures          | Any warnings      | Any errors         | `dbt test` exit code             |
| Null rate (vendor_id)      | > 1%              | > 5%               | dbt test / custom query          |
| Pipeline duration          | > 45 min          | > 90 min           | Bruin execution logs             |
| Disk usage                 | > 70%             | > 90%              | `df -h` on Docker volume         |

### 12.3 Alerting Channels

| Severity | Channel        | Response Time | Escalation                          |
|----------|----------------|---------------|--------------------------------------|
| Warning  | Slack #data-alerts | 4 hours   | Data Engineer on-call                |
| Critical | Slack + PagerDuty  | 30 min    | Data Platform Lead                   |
| P0       | PagerDuty + Phone  | 15 min    | Engineering Manager                  |

---

## 13. Data Quality Framework

### 13.1 Built-in dbt Tests

Tests are defined in `schema.yml` files across the model layers:

| Test Type        | Example                                              | Applied To         |
|------------------|------------------------------------------------------|--------------------|
| `not_null`       | `vendor_id` cannot be null                           | Staging models     |
| `accepted_values`| `service_type` must be `Green` or `Yellow`           | Intermediate       |
| `unique`         | `trip_id` must be unique                             | fact_trips         |
| `relationships`  | `pickup_location_id` must exist in `dim_zones`       | fact_trips         |

### 13.2 Source Freshness

```yaml
# models/staging/sources.yml
freshness:
  warn_after: {count: 24, period: hour}
  error_after: {count: 48, period: hour}
loaded_at_field: lpep_pickup_datetime  # or tpep_pickup_datetime
```

### 13.3 Data Quality Checks (Recommended Additions)

| Check                          | SQL / dbt Test                                     | Frequency |
|--------------------------------|----------------------------------------------------|-----------|
| No future dates                | `pickup_datetime <= CURRENT_TIMESTAMP`             | Per run   |
| Positive fare amounts          | `fare_amount >= 0`                                 | Per run   |
| Trip distance sanity           | `trip_distance BETWEEN 0 AND 500`                  | Per run   |
| Trip duration sanity           | `trip_duration_minutes BETWEEN 0 AND 1440`         | Per run   |
| Row count anomaly detection    | Compare with 30-day rolling average                | Daily     |
| Schema drift detection         | `on_schema_change: append_new_columns`             | Per run   |

### 13.4 Data Lineage Verification

```bash
# Verify model dependencies
dbt ls --resource-type model --output json

# Check for circular dependencies
dbt compile

# View full lineage in browser
dbt docs generate && dbt docs serve
```

---

## 14. Disaster Recovery & Backups

### 14.1 Backup Strategy

| Component    | Method                                        | Frequency | Retention |
|--------------|-----------------------------------------------|-----------|-----------|
| PostgreSQL   | `pg_dump` to compressed file                  | Daily     | 30 days   |
| dbt project  | Git (version controlled)                      | Per commit| Permanent |
| Docker volumes | Volume backup script                        | Weekly    | 4 weeks   |
| Pipeline config | Git (version controlled)                   | Per commit| Permanent |

### 14.2 Backup Commands

```bash
# Database backup
docker exec nyc_transport_postgres \
  pg_dump -U root -d nyc_taxi -Fc -f /tmp/nyc_taxi_backup.dump

# Copy backup from container
docker cp nyc_transport_postgres:/tmp/nyc_taxi_backup.dump ./backups/

# Restore from backup
docker exec -i nyc_transport_postgres \
  pg_restore -U root -d nyc_taxi -c /tmp/nyc_taxi_backup.dump
```

### 14.3 Recovery Time Objectives

| Scenario                    | RTO      | RPO      | Recovery Method                       |
|-----------------------------|----------|----------|---------------------------------------|
| Single model failure        | 15 min   | 0        | Re-run `dbt build --select model_name`|
| Full database corruption    | 2 hours  | 24 hours | Restore from pg_dump + re-run pipeline|
| Infrastructure failure      | 30 min   | 0        | `docker compose up -d` + full refresh |
| Source data unavailable     | N/A      | N/A      | Wait for NYC TLC to restore CDN       |

---

## 15. Access Control & Security

### 15.1 Database Roles (Production Recommendation)

| Role              | Schema Access          | Permissions                | Assigned To               |
|-------------------|------------------------|----------------------------|---------------------------|
| `dbt_service`     | raw, dev, prod         | SELECT, INSERT, CREATE     | Pipeline service account  |
| `analyst_read`    | prod                   | SELECT only                | BI analysts               |
| `admin`           | all                    | ALL PRIVILEGES             | Data platform engineers   |

### 15.2 Security Checklist

- [ ] Database credentials stored in secrets manager (not plaintext in YAML)
- [ ] Network access restricted to VPC/private network in production
- [ ] pgAdmin disabled or IP-restricted in production
- [ ] SSL/TLS enabled for database connections
- [ ] Service accounts use least-privilege principles
- [ ] Audit logging enabled on PostgreSQL
- [ ] `.bruin.yml` and `profiles.yml` added to `.gitignore` (credentials)

### 15.3 Sensitive Files (Must NOT be committed)

```
.bruin.yml              # Contains database credentials
profiles.yml            # Contains database credentials
.env                    # Environment variables
*.pem / *.key           # SSL certificates
```

---

## 16. Troubleshooting Guide

### 16.1 Common Issues

#### Issue: Ingestion fails with connection refused

```
sqlalchemy.exc.OperationalError: could not connect to server: Connection refused
```

**Cause**: PostgreSQL container is not running or not healthy.

**Resolution**:
```bash
docker compose ps                          # Check container status
docker compose up -d postgres              # Restart postgres
docker logs nyc_transport_postgres         # Check logs
```

---

#### Issue: dbt build fails with "relation does not exist"

```
relation "raw.green_tripdata" does not exist
```

**Cause**: Ingestion has not been run yet; raw tables don't exist.

**Resolution**:
```bash
bruin run assets/ingestion/ingest_taxi_data.py \
  --var '{"year": 2019, "month": 1, "taxi_type": "green"}'
bruin run assets/ingestion/ingest_taxi_data.py \
  --var '{"year": 2019, "month": 1, "taxi_type": "yellow"}'
```

---

#### Issue: Parquet download fails (HTTP 404)

```
HTTPError: 404 Client Error: Not Found
```

**Cause**: Requested year/month combination is not yet available on NYC TLC CDN.

**Resolution**: Check available data at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

---

#### Issue: Duplicate rows in fact_trips

**Cause**: Deduplication key collision or source data quality issue.

**Resolution**:
```bash
# Full refresh the incremental model
dbt build --select fact_trips --full-refresh --target prod
```

---

#### Issue: High memory usage during ingestion

**Cause**: Large parquet file loaded entirely into memory (yellow taxi can be 200MB+).

**Resolution**: Reduce chunk size in `ingest_taxi_data.py` or process in batches:
```python
df.to_sql(..., chunksize=5000)  # Reduce from 10000
```

---

#### Issue: dbt tests fail on accepted_values

**Cause**: Source data contains unexpected values not in the accepted set.

**Resolution**:
1. Investigate the raw data to understand the new values
2. Update the test definition or add handling in the staging model
3. Document the data quality finding

---

### 16.2 Diagnostic Queries

```sql
-- Check data freshness per table
SELECT
    schemaname,
    relname AS table_name,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    n_live_tup AS row_count
FROM pg_stat_user_tables
WHERE schemaname IN ('raw', 'dev', 'prod')
ORDER BY schemaname, relname;

-- Check ingested partitions
SELECT
    ingestion_year,
    ingestion_month,
    COUNT(*) AS row_count,
    MIN(lpep_pickup_datetime) AS earliest_trip,
    MAX(lpep_pickup_datetime) AS latest_trip
FROM raw.green_tripdata
GROUP BY 1, 2
ORDER BY 1, 2;

-- Check for data quality anomalies
SELECT
    service_type,
    COUNT(*) AS total_trips,
    COUNT(*) FILTER (WHERE vendor_id IS NULL) AS null_vendors,
    COUNT(*) FILTER (WHERE total_amount < 0) AS negative_totals,
    COUNT(*) FILTER (WHERE trip_distance > 500) AS extreme_distances,
    COUNT(*) FILTER (WHERE trip_duration_minutes > 1440) AS extreme_durations
FROM dev.fact_trips
GROUP BY service_type;
```

---

## 17. Change Log

| Date       | Version | Author       |
|------------|---------|--------------|
| 2026-02-19 | 1.0.0   | Dario Dang   |

---

*This document is maintained by Dario Dang. For personal and learning purpose
