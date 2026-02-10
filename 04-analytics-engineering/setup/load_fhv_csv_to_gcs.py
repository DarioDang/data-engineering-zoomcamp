import os
import sys
import time
import argparse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List

from dotenv import load_dotenv
from tqdm import tqdm

from google.api_core.exceptions import NotFound, Forbidden
from google.cloud import storage
from google.cloud import bigquery


load_dotenv()

# ----------------------------
# Defaults / Env
# ----------------------------
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
if not BUCKET_NAME:
    raise ValueError("GCP_BUCKET_NAME not found. Check your .env file.")

DEFAULT_BQ_PROJECT = os.getenv("GCP_PROJECT_ID")  
DEFAULT_BQ_DATASET = os.getenv("BQ_DATASET", "datawarehouse-nyc-de-zoomcamp")
if not DEFAULT_BQ_DATASET:
    raise ValueError("BQ_DATASET not set and no default provided.")

if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS not set in .env")

DOWNLOAD_DIR = "."
CHUNK_SIZE = 8 * 1024 * 1024

DOWNLOAD_SLEEP_SECONDS = 0.5
UPLOAD_SLEEP_SECONDS = 0.75
RETRY_BACKOFF_SECONDS = 5

MAX_DOWNLOAD_WORKERS = 4
MAX_UPLOAD_WORKERS = 4

os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# ----------------------------
# Clients
# ----------------------------
storage_client = storage.Client()
bq_client = bigquery.Client()


# ----------------------------
# Helpers
# ----------------------------

def chunked(lst: List[str], size: int):
    """Yield successive chunks from list."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def get_fhv_master_schema() -> List[bigquery.SchemaField]:
    """
    Master schema for FHV data with FLOAT64 for numeric fields
    to handle type variations across different time periods.
    """
    return [
        bigquery.SchemaField("dispatching_base_num", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pickup_datetime", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("dropOff_datetime", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("PUlocationID", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("DOlocationID", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("SR_Flag", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("Affiliated_base_number", "STRING", mode="NULLABLE"),
    ]


def ensure_bq_table_with_schema(
    project_id: str, 
    dataset_id: str, 
    table_id: str,
) -> None:
    """
    Ensure the BigQuery table exists with a consistent schema.
    Creates table only if it doesn't exist.
    """
    full_table = f"{project_id}.{dataset_id}.{table_id}"
    
    try:
        bq_client.get_table(full_table)
        print(f"✅ Table {full_table} already exists")
        return
    except NotFound:
        print(f"📋 Creating table {full_table} with consistent schema...")
    
    schema = get_fhv_master_schema()
    
    table = bigquery.Table(full_table, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="pickup_datetime",  # Partition by pickup datetime
    )
    
    bq_client.create_table(table)
    print(f"✅ Created table {full_table} with {len(schema)} fields")


def months_from_args(months: str) -> List[str]:
    """
    Accept:
      - "1,2,3"
      - "01,02,03"
      - "1-6"
      - "01-06"
    Returns ["01","02",...]
    """
    months = months.strip()
    if "-" in months:
        start_s, end_s = months.split("-", 1)
        start = int(start_s)
        end = int(end_s)
        if start < 1 or end > 12 or start > end:
            raise ValueError("Invalid month range. Example: 1-6 or 01-06")
        return [f"{m:02d}" for m in range(start, end + 1)]

    parts = [p.strip() for p in months.split(",") if p.strip()]
    out = []
    for p in parts:
        m = int(p)
        if m < 1 or m > 12:
            raise ValueError(f"Invalid month: {p}")
        out.append(f"{m:02d}")
    return out


def get_fhv_csv_url(year: int, month: str) -> str:
    """Build FHV CSV.GZ data URL from GitHub releases"""
    return f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month}.csv.gz"


def ensure_bucket(bucket_name: str) -> None:
    if not bucket_name or not bucket_name.strip():
        raise ValueError("Bucket name is empty/invalid.")

    try:
        storage_client.get_bucket(bucket_name)
        print(f"✅ Bucket '{bucket_name}' exists and is accessible.")
    except NotFound:
        storage_client.create_bucket(bucket_name)
        print(f"✅ Created bucket '{bucket_name}'")
    except Forbidden:
        print(
            f"❌ Bucket '{bucket_name}' exists but you don't have access, "
            f"or you don't have permission to check it."
        )
        sys.exit(1)


def ensure_bq_dataset(project_id: str, dataset_id: str, location: str = "US") -> None:
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = location

    try:
        bq_client.get_dataset(dataset_ref)
        print(f"✅ BigQuery dataset exists: {project_id}.{dataset_id}")
    except NotFound:
        bq_client.create_dataset(dataset_ref)
        print(f"✅ Created BigQuery dataset: {project_id}.{dataset_id} (location={location})")


def safe_remove_local(file_path: str) -> None:
    try:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        print(f"[cleanup] Could not delete {file_path}: {e}")


def download_file(year: int, month: str) -> Optional[str]:
    url = get_fhv_csv_url(year, month)
    filename = f"fhv_tripdata_{year}-{month}.csv.gz"
    file_path = os.path.join(DOWNLOAD_DIR, filename)

    try:
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            return file_path

        urllib.request.urlretrieve(url, file_path)
        time.sleep(DOWNLOAD_SLEEP_SECONDS)
        return file_path
    except Exception as e:
        print(f"[download] Failed {url}: {e}")
        return None


def verify_gcs_upload(bucket: storage.Bucket, blob_name: str) -> bool:
    return bucket.blob(blob_name).exists(storage_client)


def upload_to_gcs(
    file_path: str, 
    bucket_name: str, 
    gcs_prefix: str = "", 
    max_retries: int = 3, 
    keep_local: bool = False
) -> Optional[str]:
    """Upload to GCS and return the gs:// URI if success else None."""
    bucket = storage_client.bucket(bucket_name)
    filename = os.path.basename(file_path)

    prefix = gcs_prefix.strip("/")
    blob_name = f"{prefix}/{filename}" if prefix else filename

    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            if verify_gcs_upload(bucket, blob_name):
                if not keep_local:
                    safe_remove_local(file_path)
                return f"gs://{bucket_name}/{blob_name}"

            blob.upload_from_filename(file_path, timeout=600)

            if verify_gcs_upload(bucket, blob_name):
                time.sleep(UPLOAD_SLEEP_SECONDS)
                if not keep_local:
                    safe_remove_local(file_path)
                return f"gs://{bucket_name}/{blob_name}"

            print(f"[upload] Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"[upload] Failed {blob_name} (attempt {attempt+1}/{max_retries}): {e}")

        time.sleep(RETRY_BACKOFF_SECONDS)

    print(f"[upload] Giving up on {blob_name} after {max_retries} attempts.")
    return None


def load_csv_to_bq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    gcs_uri: str,
    write_disposition: str = "WRITE_APPEND",
) -> None:
    """
    Load CSV.GZ via staging table to handle schema variations.
    """
    full_table = f"{project_id}.{dataset_id}.{table_id}"
    staging_table = f"{full_table}_staging_{int(time.time())}"
    
    print(f"📋 Loading via staging table...")

    # Step 1: Load to staging with autodetect
    staging_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        skip_leading_rows=1,  # Skip header row
        write_disposition="WRITE_TRUNCATE",
    )

    load_job = bq_client.load_table_from_uri(
        gcs_uri, staging_table, job_config=staging_config
    )

    try:
        load_job.result()
    except Exception as e:
        print(f"❌ Failed to load to staging: {e}")
        raise

    # Step 2: Get staging table schema
    staging_table_obj = bq_client.get_table(staging_table)
    columns = [field.name for field in staging_table_obj.schema]
    
    # Step 3: Build INSERT with CAST for numeric fields only
    # Datetime fields are already TIMESTAMP, so no conversion needed
    cast_columns = []
    numeric_fields = {'PUlocationID', 'DOlocationID', 'SR_Flag'}
    
    for col in columns:
        if col in numeric_fields:
            cast_columns.append(f"CAST({col} AS FLOAT64) AS {col}")
        else:
            cast_columns.append(col)
    
    columns_str = ",\n    ".join(cast_columns)
    
    # Step 4: Insert to production
    insert_query = f"""
    INSERT INTO `{full_table}`
    SELECT {columns_str}
    FROM `{staging_table}`
    """
    
    query_job = bq_client.query(insert_query)
    query_job.result()
    
    # Step 5: Cleanup
    bq_client.delete_table(staging_table, not_found_ok=True)
    
    dest_table = bq_client.get_table(full_table)
    print(f"✅ Loaded {gcs_uri} -> {full_table} ({dest_table.num_rows} total rows)")


# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Download FHV CSV.GZ -> upload to GCS -> load to BigQuery")

    parser.add_argument("--taxi_type", required=True, choices=["yellow", "green"], help="Taxi type: yellow or green")
    parser.add_argument("--year", required=True, type=int, help="Year, e.g. 2019")
    parser.add_argument("--months", required=True, help="Months: '1-6' or '01-06' or '1,2,3' or '01,02,03'")

    parser.add_argument("--bucket", default=BUCKET_NAME, help="GCS bucket name (default from env GCP_BUCKET_NAME)")
    
    parser.add_argument("--bq_project", default=DEFAULT_BQ_PROJECT, help="BigQuery project id")
    parser.add_argument("--bq_dataset", default=DEFAULT_BQ_DATASET, help="BigQuery dataset id")
    parser.add_argument("--bq_location", default="US", help="Dataset location, e.g. US or EU")

    parser.add_argument(
        "--write_disposition",
        default="WRITE_APPEND",
        choices=["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"],
        help="How to write into BigQuery tables"
    )

    args = parser.parse_args()
    bucket_name = args.bucket
    taxi_type = args.taxi_type.lower()

    if not args.bq_project:
        args.bq_project = bq_client.project

    months = months_from_args(args.months)
    
    # Construct GCS prefix: raw/fhv/{taxi_type}/YEAR
    gcs_prefix = f"raw/fhv/{taxi_type}/{args.year}"
    
    # Table name: fhv_{taxi_type}_tripdata
    table_id = f"fhv_{taxi_type}_tripdata"

    # Ensure infra exists
    ensure_bucket(bucket_name)
    ensure_bq_dataset(args.bq_project, args.bq_dataset, location=args.bq_location)
    
    # Create table with consistent schema BEFORE loading
    ensure_bq_table_with_schema(
        args.bq_project,
        args.bq_dataset,
        table_id,
    )

    print(f"\n--- Run config ---")
    print(f"FHV {taxi_type.upper()} Data (CSV.GZ), Year: {args.year}, Months: {months}")
    print(f"GCS: gs://{bucket_name}/{gcs_prefix}/")
    print(f"BQ:  {args.bq_project}.{args.bq_dataset}.{table_id}\n")

    uploaded_uris: List[str] = []

    for month_batch in chunked(months, size=3):
        print(f"\n📦 Processing batch: {month_batch}")

        # Download batch
        downloaded_files: List[str] = []
        with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as ex:
            futures = {
                ex.submit(download_file, args.year, m): m
                for m in month_batch
            }
            for fut in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="Downloading batch",
                unit="file",
            ):
                fp = fut.result()
                if fp:
                    downloaded_files.append(fp)

        if not downloaded_files:
            print("No files downloaded in this batch, skipping.")
            continue

        # Upload batch
        with ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS) as ex:
            futures = {
                ex.submit(upload_to_gcs, fp, bucket_name, gcs_prefix, keep_local=False): fp
                for fp in downloaded_files
            }
            for fut in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="Uploading batch",
                unit="file",
            ):
                uri = fut.result()
                if uri:
                    uploaded_uris.append(uri)

        print(f"✅ Batch completed: {len(downloaded_files)} files processed")

    if not uploaded_uris:
        print("❌ No files uploaded. Exiting.")
        sys.exit(1)

    # Load into BigQuery via staging tables
    print(f"\n📤 Loading {len(uploaded_uris)} files into BigQuery...")

    for uri in sorted(uploaded_uris):
        load_csv_to_bq(
            project_id=args.bq_project,
            dataset_id=args.bq_dataset,
            table_id=table_id,
            gcs_uri=uri,
            write_disposition=args.write_disposition,
        )

    print(f"\n✅ Done! Uploaded and loaded {len(uploaded_uris)} files into BigQuery.")


if __name__ == "__main__":
    main()