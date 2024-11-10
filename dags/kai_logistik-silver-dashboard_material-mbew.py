import pendulum

from airflow import models
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pytz

PROJECT_ID = 'kai-genai-prod'
SCHEMA = 'dashboard_material'
POSTGRES_CONNECTION_ID = 'kai_postgres'
TABLE = 'mbew'
ALTER_TABLE = "material_valuation"
GCS_BUCKET = 'kai_smartsheet'
FILE_NAME = f'{TABLE}.parquet'

def push_current_timestamp():
    jakarta_tz = pytz.timezone('Asia/Jakarta')
    current_ts = datetime.now(jakarta_tz).strftime('%Y-%m-%d %H:%M:%S')
    return current_ts  # Push to XCom automatically

def build_query_and_run(**kwargs):
    ti = kwargs['ti']
    current_ts = ti.xcom_pull(task_ids='push_ts_task')

    query = f"""
        INSERT INTO {PROJECT_ID}.{SCHEMA}.{ALTER_TABLE} (valuation_area, storage_location, stock_in_transit, last_update_date_time, last_insert_to_bigquery)
        SELECT
            CAST (bwkey AS STRING) as valuation_area,
            CAST (sloc AS STRING) as storage_location,
            CAST (salk3 AS NUMERIC) as stock_in_transit,
            CAST (last_update AS DATETIME) as last_update_date_time,
            DATETIME('{current_ts}') AS last_insert_to_bigquery
        FROM bq_landing_zone.{SCHEMA}_{TABLE};

        ALTER TABLE {SCHEMA}.{ALTER_TABLE}
        SET OPTIONS (
            description = '{ALTER_TABLE}'
        );

        ALTER TABLE {SCHEMA}.{ALTER_TABLE}
		ALTER COLUMN valuation_area SET OPTIONS (description = "Valuation Area");
        ALTER TABLE {SCHEMA}.{ALTER_TABLE}
		ALTER COLUMN storage_location SET OPTIONS (description = "Storage Location");
        ALTER TABLE {SCHEMA}.{ALTER_TABLE}
		ALTER COLUMN stock_in_transit SET OPTIONS (description = "Stock in Transit");
        ALTER TABLE {SCHEMA}.{ALTER_TABLE}
		ALTER COLUMN last_update_date_time SET OPTIONS (description = "Last Update Date/Time");
    """

    # Execute pakai BigQueryInsertJobOperator
    BigQueryInsertJobOperator(
        task_id='load_to_refined_zone',
        gcp_conn_id='kai_genai_prod',
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
            }
        },
        dag=kwargs['dag']
    ).execute(context=kwargs)
    
with models.DAG(
    'dashboard_material-mbew',
    description="Doing incremental load from PostgreSQL to GCS",
    start_date=pendulum.datetime(2024, 9, 30, tz="Asia/Jakarta"),
    schedule_interval='* 1 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['Gen-AI', 'dashboard_material', 'refined'],
) as dag:

    dump_from_postgres_to_gcs = PostgresToGCSOperator(
        task_id='dump_from_postgres_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql=f"""
        SELECT
            "bwkey"::TEXT as "bwkey",
            "sloc"::TEXT as "sloc",
            "salk3"::NUMERIC as "salk3",
            "last_update"::TEXT as "last_update"
        FROM {SCHEMA}.{TABLE};
        """,
        bucket=GCS_BUCKET,
        filename=f'{SCHEMA}/{TABLE}/{FILE_NAME}',  # Nama file yang disimpan di GCS
        export_format='parquet',
        gcp_conn_id='kai_genai_prod',
        # approx_max_file_size_bytes=50 * 1024 * 1024, # split 50 MB
        # parquet_row_group_size=1000000,
    )

    load_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=GCS_BUCKET,
        source_objects=f'{SCHEMA}/{TABLE}/{FILE_NAME}',
        source_format='parquet',
        destination_project_dataset_table=f'{PROJECT_ID}.bq_landing_zone.{SCHEMA}_{TABLE}', # Dataset harus dibuat terlebih dahulu
        write_disposition='WRITE_TRUNCATE', # WRITE_TRUNCATE | WRITE_EMPTY
        skip_leading_rows=1,
        autodetect=True,
        gcp_conn_id='kai_genai_prod'
    )

    push_ts_task = PythonOperator(
        task_id='push_ts_task',
        python_callable=push_current_timestamp,
        provide_context=True
    )

    build_query_task = PythonOperator(
        task_id='build_query_task',
        python_callable=build_query_and_run,
        provide_context=True
    )

    show_progress = BashOperator(
        task_id='show_progress',
        bash_command='echo DAG finished at {{ ts_nodash }}'
    )

    # Dependency
    dump_from_postgres_to_gcs >> load_to_bigquery >> push_ts_task >> build_query_task >> show_progress
