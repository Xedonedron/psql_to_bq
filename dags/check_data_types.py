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

PROJECT_ID = 'dev-nixon-daniel'
SCHEMA = 'dashboard_investasi'
POSTGRES_CONNECTION_ID = 'kai_postgres'
TABLE = 'kas_tasklist_baru_v1'
GCS_BUCKET = 'kai_sap'
FILE_NAME = f'{TABLE}.parquet'

with models.DAG(
    'check_data_types',
    description="DAG untuk memeriksa tipe data dari sebuah table, output akan disimpan di GCS",
    start_date=pendulum.datetime(2024, 9, 30, tz="Asia/Jakarta"),
    schedule_interval='* 1 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['Gen-AI', 'TESTING'],
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
            "% Complete"::TEXT as "% Complete",
            "Start"::TEXT as "Start",
            "finish"::TEXT as "finish",
            "critical"::TEXT as "critical",
            "status"::TEXT as "status",
            "Tanggal input"::TEXT as "Tanggal input",
            "No Kontrak"::TEXT as "No Kontrak",
            "Nilai Kontrak"::TEXT as "Nilai Kontrak",
            "Realisasi Pembayaran"::TEXT as "Realisasi Pembayaran",
            "keterangan"::TEXT as "keterangan"
        FROM {SCHEMA}."{TABLE}";
        """,
        bucket=GCS_BUCKET,
        filename=f'{SCHEMA}/{TABLE}/{FILE_NAME}',  # Nama file yang disimpan di GCS
        export_format='parquet',
        gcp_conn_id='servacc_nixon',
        # approx_max_file_size_bytes=50 * 1024 * 1024, # split 50 MB
        # parquet_row_group_size=1000000,
    )

    # Dependency
    dump_from_postgres_to_gcs