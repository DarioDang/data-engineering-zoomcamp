#!/usr/bin/env bash
set -e

TAXI_TYPE=$1
YEAR=$2
MONTHS=$3

if [ -z "$TAXI_TYPE" ] || [ -z "$YEAR" ] || [ -z "$MONTHS" ]; then
  echo "Usage: ./run_taxi.sh <yellow|green> <year> <months>"
  echo "Example: ./run_taxi.sh yellow 2019 1-3"
  exit 1
fi

python load_taxi_to_gcs_bq.py \
  --taxi_type "$TAXI_TYPE" \
  --year "$YEAR" \
  --months "$MONTHS" \
  --gcs_prefix "raw/$TAXI_TYPE/$YEAR" \
  --write_disposition WRITE_APPEND