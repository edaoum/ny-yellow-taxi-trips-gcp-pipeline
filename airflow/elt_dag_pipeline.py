from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'nyc_pipeline',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

BUCKET = "ny-yellow-taxi-trips-data-buckets"
SCRIPTS = "gs://" + BUCKET + "/from-git"
DBT_GCS = "gs://" + BUCKET + "/dbt"

with DAG(
    dag_id="elt_pipeline_nyc_taxi",
    default_args=default_args,
    schedule_interval="0 23 * * 5",
    catchup=False,
    tags=["nyc_taxi", "bigquery", "dbt"],
) as dag:

    download = BashOperator(
        task_id="download_taxi_data",
        bash_command="gcloud storage cp gs://ny-yellow-taxi-trips-data-buckets/from-git/download_taxi_data.py /tmp/ && python3 /tmp/download_taxi_data.py",
    )

    load_raw = BashOperator(
        task_id="load_raw_trips_data",
        bash_command="gcloud storage cp gs://ny-yellow-taxi-trips-data-buckets/from-git/load_raw_trips_data.py /tmp/ && python3 /tmp/load_raw_trips_data.py",
    )

    run_dbt = BashOperator(
        task_id="run_dbt_transformations",
        bash_command="""
            pip install dbt-bigquery --quiet &&
            export PATH=$PATH:/opt/python3.11/bin:/root/.local/bin &&
            cd /tmp &&
            gcloud storage cp gs://ny-yellow-taxi-trips-data-buckets/dbt/nyc-taxi-dbt.tar.gz . &&
            tar -xzf nyc-taxi-dbt.tar.gz &&
            cd nyc_taxi_dbt &&
            dbt run &&
            dbt test
        """,
        env={"DBT_PROFILES_DIR": "/home/airflow/gcs/data/dbt_profiles"},
    )

    create_ml = BashOperator(
        task_id="create_ml_dataset",
        bash_command="gcloud storage cp gs://ny-yellow-taxi-trips-data-buckets/from-git/create_ml_dataset_table.py /tmp/ && python3 /tmp/create_ml_dataset_table.py",
    )

    download >> load_raw >> run_dbt >> create_ml
