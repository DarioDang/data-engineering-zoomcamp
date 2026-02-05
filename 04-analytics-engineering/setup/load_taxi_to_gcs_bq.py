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

# BigQuery client uses the project in the service account
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
        
def get_partition_field(taxi_type: str) -> str:
    taxi_type = taxi_type.lower()
    if taxi_type == "yellow":
        return "tpep_dropoff_datetime"
    if taxi_type == "green":
        return "lpep_dropoff_datetime"
    raise ValueError("taxi_type must be 'yellow' or 'green'")


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


def get_base_url(taxi_type: str, year: int) -> str:
    taxi_type = taxi_type.lower()
    if taxi_type not in {"yellow", "green"}:
        raise ValueError("taxi_type must be 'yellow' or 'green'")

    # NYC TLC CDN pattern
    return f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{year}-"


def ensure_bucket(bucket_name: str) -> None:
    if not bucket_name or not bucket_name.strip():
        raise ValueError("Bucket name is empty/invalid.")

    try:
        storage_client.get_bucket(bucket_name)  # checks existence + access
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


def download_file(taxi_type: str, year: int, month: str) -> Optional[str]:
    base_url = get_base_url(taxi_type, year)
    url = f"{base_url}{month}.csv.gz"
    filename = f"{taxi_type}_tripdata_{year}-{month}.csv.gz"
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


def upload_to_gcs(file_path: str, bucket_name: str, gcs_prefix: str = "", max_retries: int = 3) -> Optional[str]:
    """
    Upload to GCS and return the gs:// URI if success else None.
    """
    bucket = storage_client.bucket(bucket_name)
    filename = os.path.basename(file_path)

    # Normalize prefix: "" or "raw/yellow/2024/"
    prefix = gcs_prefix.strip("/")
    blob_name = f"{prefix}/{filename}" if prefix else filename

    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            if verify_gcs_upload(bucket, blob_name):
                safe_remove_local(file_path)
                return f"gs://{bucket_name}/{blob_name}"

            blob.upload_from_filename(file_path, timeout=600)

            if verify_gcs_upload(bucket, blob_name):
                time.sleep(UPLOAD_SLEEP_SECONDS)
                safe_remove_local(file_path)
                return f"gs://{bucket_name}/{blob_name}"

            print(f"[upload] Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"[upload] Failed {blob_name} (attempt {attempt+1}/{max_retries}): {e}")

        time.sleep(RETRY_BACKOFF_SECONDS)

    print(f"[upload] Giving up on {blob_name} after {max_retries} attempts.")
    return None

def get_bq_schema(taxi_type: str) -> List[bigquery.SchemaField]:
    taxi_type = taxi_type.lower()

    if taxi_type == "green":
        return [
            bigquery.SchemaField(
                "VendorID", "STRING",
                description="A code indicating the LPEP provider that provided the record. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc."
            ),
            bigquery.SchemaField(
                "lpep_pickup_datetime", "TIMESTAMP",
                description="The date and time when the meter was engaged"
            ),
            bigquery.SchemaField(
                "lpep_dropoff_datetime", "TIMESTAMP",
                description="The date and time when the meter was disengaged"
            ),
            bigquery.SchemaField(
                "store_and_fwd_flag", "STRING",
                description='Y= store and forward trip N= not a store and forward trip'
            ),
            bigquery.SchemaField(
                "RatecodeID", "STRING",
                description="The final rate code in effect at the end of the trip. 1= Standard rate 2=JFK 3=Newark 4=Nassau or Westchester 5=Negotiated fare 6=Group ride"
            ),
            bigquery.SchemaField(
                "PULocationID", "STRING",
                description="TLC Taxi Zone in which the taximeter was engaged"
            ),
            bigquery.SchemaField(
                "DOLocationID", "STRING",
                description="TLC Taxi Zone in which the taximeter was disengaged"
            ),
            bigquery.SchemaField(
                "passenger_count", "INT64",
                description="The number of passengers in the vehicle. This is a driver-entered value."
            ),
            bigquery.SchemaField(
                "trip_distance", "NUMERIC",
                description="The elapsed trip distance in miles reported by the taximeter."
            ),
            bigquery.SchemaField(
                "fare_amount", "NUMERIC",
                description="The time-and-distance fare calculated by the meter"
            ),
            bigquery.SchemaField(
                "extra", "NUMERIC",
                description="Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges"
            ),
            bigquery.SchemaField(
                "mta_tax", "NUMERIC",
                description="$0.50 MTA tax that is automatically triggered based on the metered rate in use"
            ),
            bigquery.SchemaField(
                "tip_amount", "NUMERIC",
                description="Tip amount. This field is automatically populated for credit card tips. Cash tips are not included."
            ),
            bigquery.SchemaField(
                "tolls_amount", "NUMERIC",
                description="Total amount of all tolls paid in trip."
            ),
            bigquery.SchemaField("ehail_fee", "NUMERIC", description=""),
            bigquery.SchemaField(
                "improvement_surcharge", "NUMERIC",
                description="$0.30 improvement surcharge assessed on hailed trips at the flag drop. The improvement surcharge began being levied in 2015."
            ),
            bigquery.SchemaField(
                "total_amount", "NUMERIC",
                description="The total amount charged to passengers. Does not include cash tips."
            ),
            bigquery.SchemaField(
                "payment_type", "INT64",
                description="1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip"
            ),
            bigquery.SchemaField(
                "trip_type", "STRING",
                description="1= Street-hail 2= Dispatch"
            ),
            bigquery.SchemaField(
                "congestion_surcharge", "NUMERIC",
                description="Congestion surcharge applied to trips in congested zones"
            ),
        ]

    if taxi_type == "yellow":
        return [
            bigquery.SchemaField(
                "VendorID", "STRING",
                description="A code indicating the LPEP provider that provided the record. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc."
            ),
            bigquery.SchemaField(
                "tpep_pickup_datetime", "TIMESTAMP",
                description="The date and time when the meter was engaged"
            ),
            bigquery.SchemaField(
                "tpep_dropoff_datetime", "TIMESTAMP",
                description="The date and time when the meter was disengaged"
            ),
            bigquery.SchemaField(
                "passenger_count", "INT64",
                description="The number of passengers in the vehicle. This is a driver-entered value."
            ),
            bigquery.SchemaField(
                "trip_distance", "NUMERIC",
                description="The elapsed trip distance in miles reported by the taximeter."
            ),
            bigquery.SchemaField(
                "RatecodeID", "STRING",
                description="The final rate code in effect at the end of the trip. 1= Standard rate 2=JFK 3=Newark 4=Nassau or Westchester 5=Negotiated fare 6=Group ride"
            ),
            bigquery.SchemaField(
                "store_and_fwd_flag", "STRING",
                description="TRUE = store and forward trip, FALSE = not a store and forward trip"
            ),
            bigquery.SchemaField(
                "PULocationID", "STRING",
                description="TLC Taxi Zone in which the taximeter was engaged"
            ),
            bigquery.SchemaField(
                "DOLocationID", "STRING",
                description="TLC Taxi Zone in which the taximeter was disengaged"
            ),
            bigquery.SchemaField(
                "payment_type", "INT64",
                description="1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip"
            ),
            bigquery.SchemaField(
                "fare_amount", "NUMERIC",
                description="The time-and-distance fare calculated by the meter"
            ),
            bigquery.SchemaField(
                "extra", "NUMERIC",
                description="Miscellaneous extras and surcharges."
            ),
            bigquery.SchemaField(
                "mta_tax", "NUMERIC",
                description="$0.50 MTA tax that is automatically triggered based on the metered rate in use"
            ),
            bigquery.SchemaField(
                "tip_amount", "NUMERIC",
                description="Tip amount (credit card only). Cash tips are not included."
            ),
            bigquery.SchemaField(
                "tolls_amount", "NUMERIC",
                description="Total amount of all tolls paid in trip."
            ),
            bigquery.SchemaField(
                "improvement_surcharge", "NUMERIC",
                description="$0.30 improvement surcharge assessed on hailed trips at the flag drop."
            ),
            bigquery.SchemaField(
                "total_amount", "NUMERIC",
                description="The total amount charged to passengers. Does not include cash tips."
            ),
            bigquery.SchemaField(
                "congestion_surcharge", "NUMERIC",
                description="Congestion surcharge applied to trips in congested zones"
            ),
        ]

    raise ValueError("taxi_type must be 'yellow' or 'green'")


def load_csv_to_bq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    gcs_uri: str,
    partition_field: str,
    schema: List[bigquery.SchemaField],
    write_disposition: str = "WRITE_APPEND",
) -> None:
    """
    Load a csv file from GCS into BigQuery.
    """
    full_table = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,                
        field_delimiter=",",
        allow_quoted_newlines=True,
        schema=schema,
        autodetect=False,
        write_disposition=write_disposition,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        ),
        ignore_unknown_values=True,
        max_bad_records=1000,
    )

    load_job = bq_client.load_table_from_uri(
        gcs_uri,
        destination=full_table,
        job_config=job_config,
    )

    load_job.result()  

    dest_table = bq_client.get_table(full_table)
    print(
        f"✅ Loaded {gcs_uri} -> {full_table} "
        f"({dest_table.num_rows} rows, partitioned by {partition_field})"
    )


# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Download NYC taxi csv -> upload to GCS -> load to BigQuery")

    parser.add_argument("--taxi_type", required=True, choices=["yellow", "green"], help="Taxi type: yellow or green")
    parser.add_argument("--year", required=True, type=int, help="Year, e.g. 2024")
    parser.add_argument("--months", required=True, help="Months: '1-6' or '01-06' or '1,2,3' or '01,02,03'")

    parser.add_argument("--bucket", default=BUCKET_NAME, help="GCS bucket name (default from env GCP_BUCKET_NAME)")
    parser.add_argument("--gcs_prefix", default="", help="Optional GCS prefix, e.g. raw/yellow/2024")

    parser.add_argument("--bq_project", default=DEFAULT_BQ_PROJECT, help="BigQuery project id (recommended)")
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
    partition_field = get_partition_field(args.taxi_type)
    schema = get_bq_schema(args.taxi_type)


    if not args.bq_project:
        # fallback to credentials project if env not set
        args.bq_project = bq_client.project

    months = months_from_args(args.months)

    # Ensure infra exists
    ensure_bucket(bucket_name)
    ensure_bq_dataset(args.bq_project, args.bq_dataset, location=args.bq_location)

    print(f"\n--- Run config ---")
    print(f"Taxi: {args.taxi_type}, Year: {args.year}, Months: {months}")
    print(f"GCS: gs://{bucket_name}/{args.gcs_prefix.strip('/')}/ (prefix may be empty)")
    print(f"BQ:  {args.bq_project}.{args.bq_dataset}\n")

    uploaded_uris: List[str] = []

    for month_batch in chunked(months, size=3):
        print(f"\n📦 Processing batch: {month_batch}")

        # ---- Download batch ----
        downloaded_files: List[str] = []
        with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as ex:
            futures = {
                ex.submit(download_file, args.taxi_type, args.year, m): m
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

        # ---- Upload batch ----
        with ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS) as ex:
            futures = {
                ex.submit(upload_to_gcs, fp, bucket_name, args.gcs_prefix): fp
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

        print(f"Batch completed, local files cleaned up")

    if not uploaded_uris:
        print("No files uploaded. Exiting.")
        sys.exit(1)

    # Load into BigQuery
    table_id = f"{args.taxi_type}_tripdata"

    for uri in sorted(uploaded_uris):
        load_csv_to_bq(
            project_id=args.bq_project,
            dataset_id=args.bq_dataset,
            table_id=table_id,
            gcs_uri=uri,
            partition_field=partition_field,
            schema=schema,
            write_disposition=args.write_disposition, 
        )

    print(f"\n✅ Done. Uploaded {len(uploaded_uris)} files and loaded tables into BigQuery.")


if __name__ == "__main__":
    main()


