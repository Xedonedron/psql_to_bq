import pendulum

from airflow import models
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pytz

PROJECT_ID = 'kai-genai-prod'
SCHEMA = 'dashboard_investasi'
POSTGRES_CONNECTION_ID = 'kai_postgres'
TABLE = 't_milestone_sumbagsel'
GCS_BUCKET = 'kai_smartsheet'
FILE_NAME = f'{TABLE}.parquet'

def check_row_count(**kwargs):
    # Mengambil hasil dari XCom
    row_count = kwargs['ti'].xcom_pull(task_ids='get_all_data')
    
    # Mengakses nilai dalam list (misalnya [[0]] untuk tabel kosong)
    row_count_value = row_count[0][0] if row_count else 0  # Memastikan nilai 0 jika XCom kosong
    
    # Mengembalikan nama task berdasarkan hasil row_count
    if row_count_value == 0:
        return 'show_progress'
    else:
        return 'dump_from_postgres_to_gcs'

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
            CAST(tahun as INTEGER) as tahun,
            CAST(h0 as STRING) as h0,
            CAST(h1 as STRING) as h1,
            CAST(kategori as STRING) as kategori,
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
    'dashboard_investasi-t_milestone_sumbagsel',
    description="Doing incremental load from PostgreSQL to GCS",
    start_date=pendulum.datetime(2024, 9, 30, tz="Asia/Jakarta"),
    schedule_interval='* 1 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['Gen-AI', 'dashboard_investasi', 'refined'],
) as dag:

    # Ngecek data di dalam table terlebih dahulu
    get_all_data = SQLExecuteQueryOperator(
        task_id="get_all_data",
        conn_id="kai_postgres",
        sql=f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE};",
    )

    check_and_branch = BranchPythonOperator(
        task_id='check_and_branch',
        python_callable=check_row_count,
    )

    dump_from_postgres_to_gcs = PostgresToGCSOperator(
        task_id='dump_from_postgres_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql=f"""
        SELECT
            "tahun"::INTEGER as "tahun",
            "h0"::TEXT as "h0",
            "h1"::TEXT as "h1",
            "kategori"::TEXT as "kategori"
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
    get_all_data >> check_and_branch >> dump_from_postgres_to_gcs >> load_to_bigquery >> push_ts_task >> build_query_task >> show_progress
