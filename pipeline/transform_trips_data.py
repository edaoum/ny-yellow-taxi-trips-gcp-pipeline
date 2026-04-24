# =============================================================================
# transform_trips_data.py
# Pipeline NYC Yellow Taxi — Project: ny-yellow-taxi-trips
#
# Reads the raw trips table from BigQuery and applies quality filters to
# produce a clean, analysis-ready table in the transformed_data dataset.
# The output table is fully replaced on each run (CREATE OR REPLACE).
#
# Filters applied:
#   - passenger_count > 0   : removes trips with no recorded passengers
#   - trip_distance > 0     : removes zero-distance trips (likely errors)
#   - payment_type != 6     : excludes voided trips (type 6 = voided)
#   - total_amount > 0      : removes trips with no fare (likely errors)
#
# BigQuery source : ny-yellow-taxi-trips.raw_yellowtrips.trips
# BigQuery target : ny-yellow-taxi-trips.transformed_data.cleaned_and_filtered
# =============================================================================

import logging
from google.cloud import bigquery, storage
from datetime import datetime, UTC
import io

# --- GCP project configuration ---
PROJECT_ID = "ny-yellow-taxi-trips"
BUCKET_NAME = "ny-yellow-taxi-trips-data-buckets"
GCS_LOG_FOLDER = "from-git/logs/"

# --- BigQuery table references ---
RAW_TABLE = f"{PROJECT_ID}.raw_yellowtrips.trips"
TRANSFORMED_TABLE = f"{PROJECT_ID}.transformed_data.cleaned_and_filtered"

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

# --- Transformation query ---
# CREATE OR REPLACE ensures the table is fully refreshed on every run.
# Rows are ordered by source_file to make incremental auditing easier.
TRANSFORM_QUERY = f"""
    CREATE OR REPLACE TABLE `{TRANSFORMED_TABLE}` AS
    SELECT *
    FROM `{RAW_TABLE}`
    WHERE
        passenger_count > 0          -- Remove trips with no recorded passengers
        AND trip_distance > 0        -- Remove zero-distance trips (data errors)
        AND payment_type != 6        -- Exclude voided trips (payment_type 6)
        AND total_amount > 0         -- Remove trips with no fare amount
    ORDER BY source_file;
"""


def upload_log_to_gcs():
    """
    Uploads the execution log to GCS at the end of the run.
    Always called in the finally block to ensure logs are saved even on failure.
    """
    log_filename = f"{GCS_LOG_FOLDER}transform_log_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(log_filename)
    blob.upload_from_string(log_stream.getvalue())
    logging.info(f"Log uploaded: {log_filename}")


def get_row_counts():
    """
    Compares row counts between the raw table and the transformed table.
    Useful for quickly assessing how many rows were filtered out.
    Returns a tuple (raw_count, transformed_count).
    """
    raw_count_query = f"SELECT COUNT(*) as cnt FROM `{RAW_TABLE}`"
    transformed_count_query = f"SELECT COUNT(*) as cnt FROM `{TRANSFORMED_TABLE}`"

    raw_count = list(bq_client.query(raw_count_query).result())[0].cnt
    transformed_count = list(bq_client.query(transformed_count_query).result())[0].cnt
    return raw_count, transformed_count


def transform_data():
    """
    Main function: runs the transformation query to create or replace the
    cleaned_and_filtered table from the raw trips data.

    After the query completes, logs a row count comparison between the raw
    and transformed tables so filtering impact is immediately visible in logs.
    """
    try:
        logging.info("Starting data transformation...")
        logging.info(f"Source      : {RAW_TABLE}")
        logging.info(f"Destination : {TRANSFORMED_TABLE}")

        # Run the CREATE OR REPLACE transformation query
        query_job = bq_client.query(TRANSFORM_QUERY)
        query_job.result()  # Wait for the job to complete
        logging.info(f"Table {TRANSFORMED_TABLE} created and populated successfully.")

        # Log row counts to measure filtering impact
        raw_count, transformed_count = get_row_counts()
        filtered_out = raw_count - transformed_count
        filter_pct = round((filtered_out / raw_count) * 100, 2) if raw_count > 0 else 0

        logging.info(f"Raw rows            : {raw_count:,}")
        logging.info(f"Transformed rows    : {transformed_count:,}")
        logging.info(f"Filtered out        : {filtered_out:,} ({filter_pct}%)")

    except Exception as e:
        logging.error(f"Transformation failed: {str(e)}")
    finally:
        # Log is always uploaded, even if the script crashed mid-run
        upload_log_to_gcs()


if __name__ == "__main__":
    logging.info(f"Starting transformation — {datetime.now()}")
    transform_data()
