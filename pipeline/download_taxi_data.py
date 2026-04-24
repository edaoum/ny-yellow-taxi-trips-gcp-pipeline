# =============================================================================
# download_taxi_data.py
# Pipeline NYC Yellow Taxi — Project: ny-yellow-taxi-trips
#
# Downloads NYC Yellow Taxi Parquet files from the official NYC TLC source
# (2020 → current year) and uploads them directly to Google Cloud Storage
# without storing them locally.
# Idempotent: files already present in GCS are skipped.
# An execution log is automatically saved to GCS at the end of each run.
#
# Data source : https://d37ci6vzurychx.cloudfront.net/trip-data/
# GCS destination : gs://ny-yellow-taxi-trips-data/dataset/trips/
# =============================================================================

import requests
import time
from datetime import datetime, UTC
from google.cloud import storage
import logging
import io

# --- GCP project configuration ---
PROJECT_ID = "ny-yellow-taxi-trips"
BUCKET_NAME = "ny-yellow-taxi-trips-data-buckets"
GCS_FOLDER = "dataset/trips/"         # Destination folder for Parquet files
GCS_LOG_FOLDER = "from-git/logs/"     # Destination folder for execution logs

# --- GCS client initialization ---
storage_client = storage.Client(project=PROJECT_ID)

# --- In-memory logger setup ---
# Logs are written to a memory buffer (StringIO) and uploaded to GCS
# at the end of the script, rather than being written to a local file.
log_stream = io.StringIO()
logging.basicConfig(
    stream=log_stream,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def file_exists_in_gcs(bucket_name, gcs_path):
    """
    Checks whether a file already exists in GCS.
    Used to make the script idempotent: files already in the bucket
    are not re-downloaded.
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    return blob.exists()


def upload_to_gcs(bucket_name, gcs_path, data):
    """
    Uploads binary content to GCS.
    The file is streamed directly from memory without touching local disk.
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(data)
    logging.info(f"File uploaded: gs://{bucket_name}/{gcs_path}")


def upload_log_to_gcs():
    """
    Uploads the execution log to GCS at the end of the run.
    The filename includes a timestamp to avoid overwriting previous logs.
    Always called in the finally block to ensure logs are saved even on failure.
    """
    log_filename = f"{GCS_LOG_FOLDER}download_log_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(log_filename)
    blob.upload_from_string(log_stream.getvalue())
    logging.info(f"Log uploaded: {log_filename}")


def download_histo_data():
    """
    Main function: iterates over each month from 2020 to the current year,
    downloads the corresponding Parquet file from the NYC TLC source,
    and uploads it to GCS if not already present.

    File naming convention: yellow_tripdata_YYYY-MM.parquet
    Future or missing files (HTTP 404) are silently skipped.
    A 1-second delay between each file prevents overwhelming the source server.
    """
    current_year = datetime.now().year

    try:
        for year in range(2020, current_year + 1):
            for month in range(1, 13):

                # Build source URL and GCS destination path
                file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
                gcs_path = f"{GCS_FOLDER}{file_name}"
                download_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

                # Idempotency check: skip files already in GCS
                if file_exists_in_gcs(BUCKET_NAME, gcs_path):
                    logging.info(f"{file_name} already in GCS, skipping.")
                    continue

                try:
                    logging.info(f"Downloading {file_name}...")
                    # timeout=60s prevents the script from hanging on slow files
                    response = requests.get(download_url, stream=True, timeout=60)

                    if response.status_code == 200:
                        # Upload directly from memory to GCS — no local temp file
                        upload_to_gcs(BUCKET_NAME, gcs_path, response.content)
                        logging.info(f"{file_name} uploaded successfully.")
                    elif response.status_code == 404:
                        # File not yet available (future month) or removed from source
                        logging.warning(f"{file_name} not found on source (404), skipping.")
                    else:
                        logging.error(f"Failed to download {file_name}. HTTP {response.status_code}")

                except requests.exceptions.Timeout:
                    # Timeout — file will be retried on the next run
                    logging.error(f"Timeout while downloading {file_name}.")
                except Exception as e:
                    # Any other network or GCS error — continue to next file
                    logging.error(f"Unexpected error for {file_name}: {str(e)}")

                # Brief pause between files to avoid overloading the source
                time.sleep(1)

        logging.info("Download and GCS upload completed.")

    except Exception as e:
        logging.error(f"Global error: {str(e)}")
    finally:
        # Log is always uploaded, even if the script crashed mid-run
        upload_log_to_gcs()


if __name__ == "__main__":
    logging.info(f"Starting download — {datetime.now()}")
    download_histo_data()
