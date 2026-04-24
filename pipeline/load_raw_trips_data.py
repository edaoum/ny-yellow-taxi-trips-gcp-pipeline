# =============================================================================
# load_raw_trips_data.py
# Pipeline NYC Yellow Taxi — Project: ny-yellow-taxi-trips
#
# Loads new Parquet files from GCS into the BigQuery raw table
# (raw_yellowtrips.trips). Only files not yet present in BigQuery are loaded,
# making the script fully idempotent.
#
# Loading strategy (two-step):
#   1. Load the Parquet file into a temporary table with schema auto-detection
#      to handle type inconsistencies across years (e.g. passenger_count)
#   2. Insert into the final table with explicit FLOAT64 cast on passenger_count
#      and a source_file column added for traceability
#   3. Drop the temporary table after each file
#
# GCS source      : gs://ny-yellow-taxi-trips-data/dataset/trips/
# BigQuery target : ny-yellow-taxi-trips.raw_yellowtrips.trips
# =============================================================================

import logging
from google.cloud import bigquery, storage
from datetime import datetime, UTC
import io

# --- GCP project configuration ---
PROJECT_ID = "ny-yellow-taxi-trips"
BUCKET_NAME = "ny-yellow-taxi-trips-data-buckets"
GCS_FOLDER = "dataset/trips/"
GCS_LOG_FOLDER = "from-git/logs/"

# --- BigQuery table references ---
TABLE_ID = f"{PROJECT_ID}.raw_yellowtrips.trips"
TEMP_TABLE_ID = f"{TABLE_ID}_temp"   # Temporary table used for schema auto-detection

# --- GCP clients initialization ---
bq_client = bigquery.Client(project=PROJECT_ID, location="us-central1")
storage_client = storage.Client(project=PROJECT_ID)

# --- In-memory logger setup ---
# Logs are written to a memory buffer and uploaded to GCS at the end of the run.
log_stream = io.StringIO()
logging.basicConfig(
    stream=log_stream,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def upload_log_to_gcs():
    """
    Uploads the execution log to GCS at the end of the run.
    Always called in the finally block to ensure logs are saved even on failure.
    """
    log_filename = f"{GCS_LOG_FOLDER}load_log_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(log_filename)
    blob.upload_from_string(log_stream.getvalue())
    logging.info(f"Log uploaded: {log_filename}")


def get_already_loaded_files():
    """
    Queries BigQuery to retrieve the list of source files already loaded.
    Used for idempotency: only files not yet in BigQuery will be processed.
    Returns a set of filenames (e.g. {'yellow_tripdata_2024-01.parquet', ...}).
    """
    query = f"""
        SELECT DISTINCT source_file
        FROM `{TABLE_ID}`
        WHERE source_file IS NOT NULL
    """
    query_job = bq_client.query(query)
    return {row.source_file for row in query_job.result()}


def get_gcs_files():
    """
    Lists all Parquet files currently available in the GCS source folder.
    Returns a set of filenames (basename only, not the full GCS path).
    """
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=GCS_FOLDER)
    return {
        blob.name.split("/")[-1]
        for blob in blobs
        if blob.name.endswith(".parquet")
    }


def load_file_to_bigquery(file_name):
    """
    Loads a single Parquet file from GCS into the final BigQuery table.

    Step 1 — Load into temp table with autodetect=True.
              This handles schema drift between years (e.g. passenger_count
              was INT64 in older files and FLOAT64 in newer ones).

    Step 2 — Insert from temp table into final table with:
              - Explicit FLOAT64 cast on passenger_count for consistency
              - source_file column populated with the filename for traceability

    Step 3 — Drop the temp table to keep the dataset clean.
    """
    uri = f"gs://{BUCKET_NAME}/{GCS_FOLDER}{file_name}"
    logging.info(f"Loading file: {uri}")

    # --- Step 1: load into temporary table with schema auto-detection ---
    temp_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )
    load_job = bq_client.load_table_from_uri(
        uri, TEMP_TABLE_ID, job_config=temp_job_config
    )
    load_job.result()  # Wait for the load job to complete
    logging.info(f"Loaded into temp table: {TEMP_TABLE_ID}")

    # --- Step 2: insert from temp table into the final table ---
    # passenger_count is explicitly cast to FLOAT64 to normalize across years.
    # source_file is hardcoded as a string literal for row-level traceability.
    insert_query = f"""
        INSERT INTO `{TABLE_ID}`
        SELECT
            VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            CAST(passenger_count AS FLOAT64) AS passenger_count,
            trip_distance,
            RatecodeID,
            store_and_fwd_flag,
            PULocationID,
            DOLocationID,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            airport_fee,
            "{file_name}" AS source_file
        FROM `{TEMP_TABLE_ID}`
    """
    insert_job = bq_client.query(insert_query)
    insert_job.result()  # Wait for the insert to complete
    logging.info(f"Data from {file_name} inserted into {TABLE_ID}")

    # --- Step 3: drop the temp table ---
    bq_client.delete_table(TEMP_TABLE_ID, not_found_ok=True)
    logging.info(f"Temp table dropped: {TEMP_TABLE_ID}")


def load_new_files():
    """
    Main function: compares files available in GCS against files already
    loaded in BigQuery, then loads only the new ones.

    If no new files are found, the script exits early with a log message.
    After all files are loaded, logs the total row count of the final table.
    """
    try:
        # Determine which files still need to be loaded
        gcs_files = get_gcs_files()
        already_loaded = get_already_loaded_files()
        new_files = gcs_files - already_loaded

        if not new_files:
            logging.info("No new files to load. BigQuery table is up to date.")
            return

        logging.info(f"{len(new_files)} new file(s) to load: {sorted(new_files)}")

        # Load each new file one by one
        for file_name in sorted(new_files):
            try:
                load_file_to_bigquery(file_name)
            except Exception as e:
                # Log the error and continue with the next file
                logging.error(f"Failed to load {file_name}: {str(e)}")

        # Final row count for verification
        destination_table = bq_client.get_table(TABLE_ID)
        logging.info(f"Total rows in {TABLE_ID}: {destination_table.num_rows}")

    except Exception as e:
        logging.error(f"Global error during loading process: {str(e)}")
    finally:
        # Log is always uploaded, even if the script crashed mid-run
        upload_log_to_gcs()


if __name__ == "__main__":
    logging.info(f"Starting load process — {datetime.now()}")
    load_new_files()
