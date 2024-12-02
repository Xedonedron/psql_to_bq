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
SCHEMA = 'dashboard_investasi'
POSTGRES_CONNECTION_ID = 'kai_postgres'
TABLE = 'kas_tasklist_baru_beta'
GCS_BUCKET = 'kai_sap'
FILE_NAME = f'{TABLE}.parquet'

def push_current_timestamp():
    jakarta_tz = pytz.timezone('Asia/Jakarta')
    current_ts = datetime.now(jakarta_tz).strftime('%Y-%m-%d %H:%M:%S')
    return current_ts  # Push to XCom automatically


def build_query_and_run(**kwargs):
    ti = kwargs['ti']
    current_ts = ti.xcom_pull(task_ids='push_ts_task')

    query = f"""
        CREATE or REPLACE TABLE {PROJECT_ID}.{SCHEMA}.{TABLE} as
        SELECT
            CAST(`WBS ID Lv 5` AS STRING) AS `WBS ID Lv 5`,
            CAST(`WBS Desc Lv 5` AS STRING) AS `WBS Desc Lv 5`,
            CAST(`WBS ID` AS STRING) AS `WBS ID`,
            CAST(`Task Name` AS STRING) AS `Task Name`,
            CAST(`wpc` AS STRING) AS `wpc`,
            CAST(`% Complete` AS NUMERIC) AS `% Complete`,
            CAST(`Start` AS DATE) AS `Start`,
            CAST(`finish` AS DATE) AS `finish`,
            CAST(`critical` AS STRING) AS `critical`,
            CAST(`status` AS STRING) AS `status`,
            CAST(`Tanggal input` AS DATE) AS `Tanggal input`,
            CAST(`No Kontrak` AS STRING) AS `No Kontrak`,
            CAST(`Nilai Kontrak` AS INTEGER) AS `Nilai Kontrak`,
            CAST(`Realisasi Pembayaran` AS INTEGER) AS `Realisasi Pembayaran`,
            CAST(`keterangan` AS STRING) as `keterangan`,
            DATETIME('{current_ts}') AS last_insert_to_bigquery
        FROM bq_landing_zone.{SCHEMA}_{TABLE};
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
    'dashboard_investasi-kas_tasklist_baru_beta',
    description="Doing incremental load from PostgreSQL to GCS",
    start_date=pendulum.datetime(2024, 9, 30, tz="Asia/Jakarta"),
    schedule_interval='0 3 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['Gen-AI', 'dashboard_investasi', 'refined'],
) as dag:

    dump_from_postgres_to_gcs = PostgresToGCSOperator(
        task_id='dump_from_postgres_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql=f"""
        SELECT
            "WBS ID Lv 5"::TEXT as "WBS ID Lv 5",
            "WBS Desc Lv 5"::TEXT as "WBS Desc Lv 5",
            "WBS ID"::TEXT as "WBS ID",
            "Task Name"::TEXT as "Task Name",
            "wpc"::TEXT as "wpc",
            "% Complete"::DOUBLE PRECISION as "% Complete",
            "Start"::TEXT as "Start",
            "finish"::TEXT as "finish",
            "critical"::TEXT as "critical",
            "status"::TEXT as "status",
            "Tanggal input"::TEXT as "Tanggal input",
            "No Kontrak"::TEXT as "No Kontrak",
            "Nilai Kontrak"::BIGINT as "Nilai Kontrak",
            "Realisasi Pembayaran"::BIGINT as "Realisasi Pembayaran",
            "keterangan"::TEXT as "keterangan"
        FROM {SCHEMA}."{TABLE}";
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
