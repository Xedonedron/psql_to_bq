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
TABLE = 'master_komponen'
ALTER_TABLE = "tabel_harga_satuan_per_plant"
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
        INSERT INTO {PROJECT_ID}.{SCHEMA}.{ALTER_TABLE} (component_id, plant, material_id, component_name, unit, procurement, balance, total_balance_price, unit_price, created_at, created_by, last_update, last_insert_to_bigquery)
        SELECT
            CAST(id_komponen as INTEGER) as component_id,
            CAST(werks as STRING) as plant,
            CAST(id_material as STRING) as material_id,
            CAST(nama_komponen as STRING) as component_name,
            CAST(satuan as STRING) as unit,
            CAST(pengadaan as STRING) as procurement,
            CAST(saldo as NUMERIC) as balance,
            CAST(harga_total_saldo as NUMERIC) as total_balance_price,
            CAST(harga_satuan as NUMERIC) as unit_price,
            CAST(created_at as STRING) as created_at,
            CAST(created_by as STRING) as created_by,
            CAST(last_update as DATETIME) as last_update,
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
    'dashboard_material-master_komponen',
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
            "id_komponen"::BIGINT as "id_komponen",
            "werks"::TEXT as "werks",
            "id_material"::TEXT as "id_material",
            "nama_komponen"::TEXT as "nama_komponen",
            "satuan"::TEXT as "satuan",
            "pengadaan"::TEXT as "pengadaan",
            "saldo"::NUMERIC as "saldo",
            "harga_total_saldo"::NUMERIC as "harga_total_saldo",
            "harga_satuan"::NUMERIC as "harga_satuan",
            "created_at"::TEXT as "created_at",
            "created_by"::TEXT as "created_by",
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
