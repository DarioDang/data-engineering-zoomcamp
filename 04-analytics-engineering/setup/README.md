# NYC Transportation Data Pipeline

A comprehensive data engineering pipeline for processing NYC taxi and For-Hire Vehicle (FHV) data, built with modern data stack tools and best practices.

## Overview

This project implements an end-to-end ETL pipeline that extracts NYC transportation data from multiple sources, loads it into Google Cloud Platform infrastructure, and transforms it for analysis. The pipeline handles both regular taxi data (yellow and green) and For-Hire Vehicle data across multiple years.

## Architecture

```
Source Data → Extract (Python) → GCS (Raw Storage) → BigQuery (Data Warehouse) → dbt (Transformation) → Analytics-Ready Data
```

### Technology Stack

- **Extraction & Loading**: Python with PyArrow
- **Cloud Infrastructure**: Google Cloud Platform (GCS, BigQuery)
- **Transformation**: dbt (staging-to-production architecture)
- **Orchestration**: Shell scripts with environment variables
- **Concurrency**: ThreadPoolExecutor for parallel processing

## Features

- **Multi-Source Data Ingestion**
  - Parquet files from NYC's CloudFront
  - CSV.GZ files from GitHub releases
  
- **Robust Data Processing**
  - Concurrent downloads with ThreadPoolExecutor
  - Retry logic for resilient operations
  - Comprehensive error handling
  
- **Schema Handling**
  - Staging table approach to manage schema inconsistencies
  - Autodetect schemas with proper type casting
  - Handles temporal schema variations in source data

- **Organized Storage**
  - Consistent GCS folder hierarchy: `raw/fhv/{taxi_type}/{year}`
  - Systematic BigQuery naming: `fhv_{taxi_type}_tripdata`
  - Clear separation between taxi and FHV data types

## Project Structure

```
.
├── load_fhv_csv_to_gcs.py      # FHV data ingestion pipeline
├── load_taxi_to_gcs_bq.py      # Taxi data ingestion pipeline
├── run_fhv_csv.sh              # FHV pipeline execution wrapper
├── run_taxi.sh                 # Taxi pipeline execution wrapper
└── pyproject.toml              # Python dependencies
```

## Setup

### Prerequisites

- Python 3.9+
- Google Cloud Platform account with:
  - Cloud Storage bucket
  - BigQuery dataset
  - Appropriate IAM permissions
- dbt installed and configured
