# =============================================================================
# create_ml_dataset_table.py
# Pipeline NYC Yellow Taxi — Project: ny-yellow-taxi-trips
#
# Creates the ML training dataset by filtering the cleaned trips table.
# The output table is fully replaced on each run (CREATE OR REPLACE),
# ensuring it always reflects the latest available data.
#
# Filters applied:
#   - tpep_pickup_datetime >= 2024-11-01 : recent data only for ML relevance
#   - EXTRACT(YEAR) BETWEEN 2024 AND current year : excludes future anomalies
#   - payment_type IN (1, 2) : card and cash only — ensures total_amount
#     is a reliable label (other types may have zero or negative amounts)
#
# The target variable for all ML models is: total_amount
#
# BigQuery source : ny-yellow-taxi-trips.transformed_data.cleaned_and_filtered
# BigQuery target : ny-yellow-taxi-trips.ml_dataset.trips_ml_data
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
TRANSFORMED_TABLE = f"{PROJECT_ID}.transformed_data.cleaned_and_filtered"
ML_TABLE = f"{PROJECT_ID}.ml_dataset.trips_ml_data"

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

# --- ML dataset creation query ---
# Filters to recent trips with reliable payment types only.
# payment_type 1 = Credit card, 2 = Cash.
# These are the only types where total_amount is consistently recorded,
# making them suitable as a regression label for ML training.
ML_QUERY = f"""
    CREATE OR REPLACE TABLE `{ML_TABLE}` AS
    SELECT *
    FROM `{TRANSFORMED_TABLE}`
    WHERE
        tpep_pickup_datetime >= TIMESTAMP('2024-11-01')
        AND EXTRACT(YEAR FROM tpep_pickup_datetime)
            BETWEEN 2024 AND EXTRACT(YEAR FROM CURRENT_DATE())
        AND payment_type IN (1, 2);  -- Card (1) and cash (2) only
"""


def upload_log_to_gcs():
    """
    Uploads the execution log to GCS at the end of the run.
    Always called in the finally block to ensure logs are saved even on failure.
    """
    log_filename = f"{GCS_LOG_FOLDER}ml_table_log_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(log_filename)
    blob.upload_from_string(log_stream.getvalue())
    logging.info(f"Log uploaded: {log_filename}")


def get_row_count(table_id):
    """
    Returns the total row count of a given BigQuery table.
    Used after the ML table is created to verify data volume.

    Args:
        table_id (str): Fully qualified BigQuery table ID.

    Returns:
        int: Number of rows in the table.
    """
    query = f"SELECT COUNT(*) as cnt FROM `{table_id}`"
    result = list(bq_client.query(query).result())
    return result[0].cnt


def create_ml_data():
    """
    Main function: runs the ML dataset creation query to populate
    trips_ml_data from the cleaned_and_filtered table.

    Logs row counts from both source and ML tables after completion
    so the filtering impact is immediately visible in logs.
    """
    try:
        logging.info("Starting ML dataset table creation...")
        logging.info(f"Source      : {TRANSFORMED_TABLE}")
        logging.info(f"Destination : {ML_TABLE}")

        # Run the CREATE OR REPLACE query to build the ML table
        query_job = bq_client.query(ML_QUERY)
        query_job.result()  # Wait for the job to complete
        logging.info(f"Table {ML_TABLE} created and populated successfully.")

        # Log row counts to verify filtering impact
        source_count = get_row_count(TRANSFORMED_TABLE)
        ml_count = get_row_count(ML_TABLE)
        kept_pct = round((ml_count / source_count) * 100, 2) if source_count > 0 else 0

        logging.info(f"Source rows (cleaned) : {source_count:,}")
        logging.info(f"ML table rows         : {ml_count:,}")
        logging.info(f"Kept for ML training  : {kept_pct}% of cleaned data")

    except Exception as e:
        logging.error(f"ML table creation failed: {str(e)}")
    finally:
        # Log is always uploaded, even if the script crashed mid-run
        upload_log_to_gcs()


if __name__ == "__main__":
    logging.info(f"Starting ML dataset creation — {datetime.now()}")
    create_ml_data()
