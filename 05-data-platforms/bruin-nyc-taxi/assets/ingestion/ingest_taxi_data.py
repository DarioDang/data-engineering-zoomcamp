"""@bruin
name: ingest_taxi_data
type: python
connection: postgres_local
depends: []

parameters:
  year: 2019
  month: 1
  taxi_type: green
@bruin"""

import pandas as pd
import os
import json
from sqlalchemy import create_engine, text

# Get Bruin connection from environment
def get_connection():
    """Get database connection string from Bruin environment"""
    # Bruin sets connection details in environment variables
    # For postgres, it provides the full connection string
    conn_user = os.getenv('BRUIN_POSTGRES_USERNAME', 'root')
    conn_pass = os.getenv('BRUIN_POSTGRES_PASSWORD', 'root')
    conn_host = os.getenv('BRUIN_POSTGRES_HOST', 'localhost')
    conn_port = os.getenv('BRUIN_POSTGRES_PORT', '5432')
    conn_db = os.getenv('BRUIN_POSTGRES_DATABASE', 'nyc_taxi')

    return f"postgresql://{conn_user}:{conn_pass}@{conn_host}:{conn_port}/{conn_db}"

# Get parameters from BRUIN_VARS
bruin_vars = json.loads(os.getenv('BRUIN_VARS', '{}'))
year = str(bruin_vars.get('year', '2019'))
month = str(bruin_vars.get('month', '1'))
taxi_type = str(bruin_vars.get('taxi_type', 'green')).lower()

# Validate taxi_type
if taxi_type not in ['green', 'yellow']:
    raise ValueError(f"Invalid taxi_type: {taxi_type}. Must be 'green' or 'yellow'")

# Format month to 2 digits
month_str = str(month).zfill(2)

# NYC Taxi data URL
url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month_str}.parquet"

print(f"Downloading {taxi_type.upper()} taxi data from: {url}")

# Read parquet file
df = pd.read_parquet(url)

print(f"Downloaded {len(df)} rows of {taxi_type} taxi data")

# Add metadata columns
df['ingestion_year'] = int(year)
df['ingestion_month'] = int(month)

# Get connection and create engine
connection_string = get_connection()
engine = create_engine(connection_string)

# Table details
table_name = f"{taxi_type}_tripdata"
schema = "raw"

# Create schema if needed
with engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    conn.commit()

# Check for existing data
try:
    with engine.connect() as conn:
        check_query = text(f"""
            SELECT COUNT(*) as count
            FROM {schema}.{table_name}
            WHERE ingestion_year = {year} AND ingestion_month = {month}
        """)
        result = conn.execute(check_query)
        existing_count = result.fetchone()[0]

        if existing_count > 0:
            print(f"⚠️  Found {existing_count} existing rows for {year}-{month_str}, deleting...")
            delete_query = text(f"""
                DELETE FROM {schema}.{table_name}
                WHERE ingestion_year = {year} AND ingestion_month = {month}
            """)
            conn.execute(delete_query)
            conn.commit()
            print(f"✓ Deleted {existing_count} existing rows")
except Exception as e:
    print(f"ℹ️  Table doesn't exist yet: {e}")

# Append to database
df.to_sql(
    table_name,
    engine,
    schema=schema,
    if_exists='append',
    index=False,
    method='multi',
    chunksize=10000
)

print(f"✓ {taxi_type.upper()} taxi data appended to {schema}.{table_name}")
print(f"✓ Period: {year}-{month_str}")
print(f"✓ Rows inserted: {len(df)}")

# Show total
with engine.connect() as conn:
    total_query = text(f"SELECT COUNT(*) FROM {schema}.{table_name}")
    result = conn.execute(total_query)
    total_rows = result.fetchone()[0]
    print(f"✓ Total rows in table: {total_rows}")




