# =============================================================================
# create_datasets.py
# Pipeline NYC Yellow Taxi — Project: ny-yellow-taxi-trips
#
# Creates all BigQuery datasets required by the pipeline.
# Safe to run multiple times: existing datasets are detected and skipped,
# making this script fully idempotent.
#
# Datasets created:
#   - raw_yellowtrips    : raw Parquet data loaded from GCS
#   - transformed_data   : cleaned and filtered trips table
#   - views_fordashboard : analytical SQL views for Looker Studio
#   - dbt_staging        : dbt staging layer (views)
#   - dbt_marts          : dbt marts layer (materialized tables)
#   - ml_dataset         : filtered dataset used for BigQuery ML training
#
# All datasets are created in the US multi-region location to match
# the GCS bucket region and avoid cross-region query costs.
# =============================================================================

import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# --- GCP project configuration ---
PROJECT_ID = "ny-yellow-taxi-trips"
LOCATION = "US"

# --- List of all datasets to create ---
# To add a new dataset to the pipeline, simply append its name here.
DATASETS = [
    "raw_yellowtrips",
    "transformed_data",
    "views_fordashboard",
    "dbt_staging",
    "dbt_marts",
    "ml_dataset",
]

# --- BigQuery client initialization ---
bq_client = bigquery.Client(project=PROJECT_ID)

# --- Basic logger setup ---
# Outputs to stdout — visible directly in Cloud Shell or Airflow task logs.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_dataset(dataset_name):
    """
    Creates a single BigQuery dataset if it does not already exist.
    Checks for existence first to avoid raising an error on re-runs.

    Args:
        dataset_name (str): The ID of the dataset to create (e.g. 'raw_yellowtrips').
    """
    dataset_id = f"{PROJECT_ID}.{dataset_name}"

    try:
        # Check if the dataset already exists
        bq_client.get_dataset(dataset_id)
        logging.info(f"Dataset already exists, skipping: {dataset_id}")

    except NotFound:
        # Dataset does not exist — create it
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = LOCATION
        bq_client.create_dataset(dataset, timeout=30)
        logging.info(f"Dataset created: {dataset_id} (location: {LOCATION})")


def create_all_datasets():
    """
    Iterates over the DATASETS list and creates each one.
    Logs a summary at the end with the total count of datasets processed.
    """
    logging.info(f"Starting dataset creation for project: {PROJECT_ID}")
    logging.info(f"Datasets to process: {DATASETS}")

    for dataset_name in DATASETS:
        create_dataset(dataset_name)

    logging.info(f"Done — {len(DATASETS)} dataset(s) processed.")


if __name__ == "__main__":
    create_all_datasets()
