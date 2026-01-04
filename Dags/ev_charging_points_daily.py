from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator, 
    BigQueryColumnCheckOperator,
    BigQueryValueCheckOperator
)
from datetime import datetime, timedelta
from logic import extract

# Configuration
BUCKET_NAME = 'us-central1-nimbus-aa74b3d5-bucket'
PROJECT_ID = 'project-ad7287ce-8651-4583-931'
DATASET_ID = 'nimbus'
LANDING_TABLE = "`project-ad7287ce-8651-4583-931.nimbus.central_london_ev_charging_points_landing`"
TARGET_TABLE = "`project-ad7287ce-8651-4583-931.nimbus.central_london_ev_charging_points_target`"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ev_charging_points_daily',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # Extraction from API
    extraction_task = PythonOperator(
        task_id='run_ev_extraction',
        python_callable=extract,
    )

    # Load to Landing Table (Write Truncate)
    load_to_bq_landing = GCSToBigQueryOperator(
        task_id='load_to_bigquery_landing',
        bucket=BUCKET_NAME,
        source_objects=['data/ev_charging_data_normalized.csv'],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.central_london_ev_charging_points_landing",
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    # Landing Quality Check: Validating Coordinates and Nulls
    landing_column_checks = BigQueryColumnCheckOperator(
        task_id="check_landing_columns",
        table=LANDING_TABLE,
        column_mapping={
            "ID": {"null_check": {"equal_to": 0}},
            "conn_ID": {"null_check": {"equal_to": 0}},
            "AddressInfo_Latitude": {
                "min": {"greater_than": 50.0}, 
                "max": {"less_than": 53.0}
            },
            "AddressInfo_Longitude": {
                "min": {"greater_than": -0.5}, 
                "max": {"less_than": 0.5}
            },
        },
        use_legacy_sql=False,
    )

    # Landing Quality Check: Duplicate Detection
    landing_duplicate_check = BigQueryValueCheckOperator(
        task_id="check_landing_duplicates",
        sql=f"SELECT COUNT(conn_ID) - COUNT(DISTINCT conn_ID) FROM {LANDING_TABLE}",
        pass_value=0,
        use_legacy_sql=False,
    )

    # Transformation via Stored Procedure
    transform_to_target = BigQueryInsertJobOperator(
        task_id='transform_to_target',
        configuration={
            "query": {
                "query": f"CALL `{PROJECT_ID}.{DATASET_ID}.sp_transform_ev_data`()",
                "useLegacySql": False,
            }
        },
    )

    # Target Quality Check: Ensuring post-transform health
    target_quality_check = BigQueryColumnCheckOperator(
        task_id="check_target_data_quality",
        table=TARGET_TABLE,
        column_mapping={
            "station_id": {"null_check": {"equal_to": 0}},
            "power_kw": {"min": {"geq_to": 0}}, 
        },
        use_legacy_sql=False
    )

    # Archive to GCS
    archive_to_gcs = GCSToGCSOperator(
        task_id='archive_to_gcs',
        source_bucket=BUCKET_NAME,
        source_object='data/ev_charging_data_normalized.csv',
        destination_bucket=BUCKET_NAME,
        destination_object='archive/ev_charging_data_{{ ds }}.csv',
        move_object=True,
    )

    # Dependency Graph
    extraction_task >> load_to_bq_landing 
    load_to_bq_landing >> [landing_column_checks, landing_duplicate_check]
    [landing_column_checks, landing_duplicate_check] >> transform_to_target 
    transform_to_target >> target_quality_check >> archive_to_gcs