#!/usr/bin/env bash
set -e

TAXI_TYPE=$1
YEAR=$2
MONTHS=$3

if [ -z "$TAXI_TYPE" ] || [ -z "$YEAR" ] || [ -z "$MONTHS" ]; then
  echo "Usage: ./run_fhv_csv.sh <yellow|green> <year> <months>"
  echo "Example: ./run_fhv_csv.sh green 2019 1-12"
  exit 1
fi

python load_fhv_csv_to_gcs.py \
  --taxi_type "$TAXI_TYPE" \
  --year "$YEAR" \
  --months "$MONTHS" \
  --write_disposition WRITE_APPEND