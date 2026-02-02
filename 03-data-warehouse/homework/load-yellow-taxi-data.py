import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
from typing import Optional
from tqdm import tqdm

load_dotenv()

# ----------------------------
# Config
# ----------------------------
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
if not BUCKET_NAME:
    raise ValueError("GCP_BUCKET_NAME not found. Check your .env file.")

CREDENTIALS_FILE = "../service-account.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]
DOWNLOAD_DIR = "."
CHUNK_SIZE = 8 * 1024 * 1024

# Sleep to avoid hammering services
DOWNLOAD_SLEEP_SECONDS = 0.5      # between download requests
UPLOAD_SLEEP_SECONDS = 0.75       # between upload requests
RETRY_BACKOFF_SECONDS = 5         # between retry attempts

# Concurrency
MAX_DOWNLOAD_WORKERS = 4
MAX_UPLOAD_WORKERS = 4

os.makedirs(DOWNLOAD_DIR, exist_ok=True)



# Helpers
def create_bucket(bucket_name: str) -> None:
    """Ensure bucket exists and belongs to your project (or create if missing)."""
    if not bucket_name or not bucket_name.strip():
        raise ValueError("Bucket name is empty/invalid.")

    try:
        bucket = client.get_bucket(bucket_name)

        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding...")
        else:
            print(f"A bucket named '{bucket_name}' exists but does not belong to your project.")
            sys.exit(1)

    except NotFound:
        client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        print(
            f"A bucket named '{bucket_name}' exists but is not accessible. "
            f"Name may be taken or you lack access. Use a different bucket name."
        )
        sys.exit(1)


def download_file(month: str) -> Optional[str]:
    """Download one month parquet to local disk. Returns local file path or None."""
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")

    try:
        # Skip if already exists locally (nice for reruns)
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            return file_path

        urllib.request.urlretrieve(url, file_path)
        time.sleep(DOWNLOAD_SLEEP_SECONDS)  # throttle to avoid request bursts
        return file_path
    except Exception as e:
        print(f"[download] Failed {url}: {e}")
        return None


def verify_gcs_upload(bucket: storage.Bucket, blob_name: str) -> bool:
    return bucket.blob(blob_name).exists(client)


def upload_to_gcs(file_path: str, max_retries: int = 3) -> bool:
    """Upload a local file to GCS with retries + verification. Returns True if uploaded."""
    bucket = client.bucket(BUCKET_NAME)
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            # If already exists in GCS, skip upload and delete local file
            if verify_gcs_upload(bucket, blob_name):
                # delete local to keep disk clean
                safe_remove_local(file_path)
                return True

            blob.upload_from_filename(file_path, timeout=600)

            # Verification
            if verify_gcs_upload(bucket, blob_name):
                time.sleep(UPLOAD_SLEEP_SECONDS)  # throttle to avoid bursts
                safe_remove_local(file_path)      # auto delete local file after success
                return True

            print(f"[upload] Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"[upload] Failed {blob_name} (attempt {attempt+1}/{max_retries}): {e}")

        time.sleep(RETRY_BACKOFF_SECONDS)

    print(f"[upload] Giving up on {blob_name} after {max_retries} attempts.")
    return False


def safe_remove_local(file_path: str) -> None:
    """Delete local file if it exists; ignore errors."""
    try:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        print(f"[cleanup] Could not delete {file_path}: {e}")



# Main
if __name__ == "__main__":
    create_bucket(BUCKET_NAME)
    print(f"Using bucket: {BUCKET_NAME}")

    # -------- Downloads with tqdm --------
    downloaded_files: list[str] = []
    with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as ex:
        futures = {ex.submit(download_file, m): m for m in MONTHS}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Downloading", unit="file"):
            fp = fut.result()
            if fp:
                downloaded_files.append(fp)

    if not downloaded_files:
        print("No files downloaded. Exiting.")
        sys.exit(1)

    # -------- Uploads with tqdm --------
    success = 0
    with ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS) as ex:
        futures = {ex.submit(upload_to_gcs, fp): fp for fp in downloaded_files}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Uploading", unit="file"):
            ok = fut.result()
            success += int(ok)

    print(f"All files processed. Successful uploads: {success}/{len(downloaded_files)}")