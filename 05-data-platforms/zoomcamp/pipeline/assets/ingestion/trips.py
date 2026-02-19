"""@bruin
name: ingestion.trips
type: python
connection: pg-zoomcamp
@bruin"""

import os
import json
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine, text


# Column mappings: normalize pickup/dropoff datetime names per taxi type
COLUMN_RENAME = {
    "green": {
        "lpep_pickup_datetime": "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
        "VendorID": "vendor_id",
        "RatecodeID": "rate_code_id",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
    },
    "yellow": {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "VendorID": "vendor_id",
        "RatecodeID": "rate_code_id",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
    },
}

OUTPUT_COLUMNS = [
    "vendor_id",
    "service_type",
    "pickup_datetime",
    "dropoff_datetime",
    "store_and_fwd_flag",
    "rate_code_id",
    "pickup_location_id",
    "dropoff_location_id",
    "passenger_count",
    "trip_distance",
    "trip_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "ehail_fee",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "total_amount",
    "payment_type",
    "extracted_at",
]

SCHEMA = "raw"


def get_connection_string() -> str:
    user   = os.getenv("BRUIN_POSTGRES_USERNAME", "root")
    passwd = os.getenv("BRUIN_POSTGRES_PASSWORD", "root")
    host   = os.getenv("BRUIN_POSTGRES_HOST",     "localhost")
    port   = os.getenv("BRUIN_POSTGRES_PORT",     "5433")
    db     = os.getenv("BRUIN_POSTGRES_DATABASE", "nyc_taxi")
    return f"postgresql://{user}:{passwd}@{host}:{port}/{db}"


def fetch_taxi_data(taxi_type: str, year: int, month: int) -> pd.DataFrame:
    month_str = str(month).zfill(2)
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month_str}.parquet"
    print(f"  Downloading {taxi_type} taxi {year}-{month_str}: {url}")

    df = pd.read_parquet(url)
    print(f"  Downloaded {len(df):,} rows")

    df = df.rename(columns=COLUMN_RENAME[taxi_type])
    df["service_type"] = taxi_type
    df["extracted_at"] = datetime.now(timezone.utc).replace(tzinfo=None)

    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None

    return df[OUTPUT_COLUMNS]


# --- Parse run window from Bruin environment ---
start_date_str = os.getenv("BRUIN_START_DATE")
end_date_str   = os.getenv("BRUIN_END_DATE")

if not start_date_str or not end_date_str:
    raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be set.")

start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
end_date   = datetime.strptime(end_date_str,   "%Y-%m-%d")
print(f"Run window: {start_date_str} -> {end_date_str}")

# --- Read pipeline variable ---
bruin_vars = json.loads(os.getenv("BRUIN_VARS", "{}"))
taxi_types = bruin_vars.get("taxi_types", ["green"])
if isinstance(taxi_types, str):
    taxi_types = [taxi_types]
print(f"Taxi types: {taxi_types}")

# --- Determine which year-months to fetch ---
months_to_fetch = set()
current = start_date.replace(day=1)
while current <= end_date:
    months_to_fetch.add((current.year, current.month))
    if current.month == 12:
        current = current.replace(year=current.year + 1, month=1)
    else:
        current = current.replace(month=current.month + 1)

# --- Write to PostgreSQL ---
engine = create_engine(get_connection_string())

with engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
    conn.commit()

# --- Download, filter, and write per taxi type ---
for taxi_type in taxi_types:
    table = f"{taxi_type}_tripdata"

    frames = []
    for year, month in sorted(months_to_fetch):
        df = fetch_taxi_data(taxi_type, year, month)
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        mask = (df["pickup_datetime"] >= start_date) & (df["pickup_datetime"] < end_date)
        df = df[mask]
        print(f"  Filtered to {len(df):,} rows for {taxi_type} {year}-{str(month).zfill(2)}")
        frames.append(df)

    if not frames or all(f.empty for f in frames):
        print(f"No data found for {taxi_type} in the requested window — skipping.")
        continue

    result = pd.concat(frames, ignore_index=True)
    print(f"{taxi_type}: {len(result):,} rows to load into {SCHEMA}.{table}")

    # Idempotent: delete this window's rows if the table already exists
    with engine.connect() as conn:
        table_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = :schema AND table_name = :table
            )
        """), {"schema": SCHEMA, "table": table}).scalar()

        if table_exists:
            conn.execute(text(f"""
                DELETE FROM {SCHEMA}.{table}
                WHERE pickup_datetime >= :start AND pickup_datetime < :end
            """), {"start": start_date, "end": end_date})
            conn.commit()
            print(f"  Deleted existing rows for window {start_date_str} -> {end_date_str}")
        else:
            print(f"  Table {SCHEMA}.{table} does not exist yet — will be created.")

    result.to_sql(
        table,
        engine,
        schema=SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=10_000,
    )
    print(f"  Loaded {len(result):,} rows into {SCHEMA}.{table}")

    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA}.{table}")).scalar()
        print(f"  Total rows in {SCHEMA}.{table}: {total:,}")


