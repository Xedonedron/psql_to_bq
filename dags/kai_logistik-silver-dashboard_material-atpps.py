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
from time import sleep

PROJECT_ID = 'kai-genai-prod'
SCHEMA = 'dashboard_material'
POSTGRES_CONNECTION_ID = 'kai_postgres'
TABLE = 'atpps'
ALTER_TABLE = "tabel_awal_terima_pakai_sisa"
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
        INSERT INTO {PROJECT_ID}.{SCHEMA}.{ALTER_TABLE} (client_id, material_number, plant, storage_location, batch_number, material_description, material_type, material_group, valuation_class, unit_of_measure, initial_stock, received_quantity, total_received_quantity, total_received_amount, quantity_out, total_amount_out, ending_stock, ending_amount, status, year, last_update, last_insert_to_bigquery)
        SELECT
            CAST (MANDT AS STRING) AS client_id,
            CAST (MATNR AS STRING) AS material_number,
            CAST (WERKS AS STRING) AS plant,
            CAST (LGORT AS STRING) AS storage_location,
            CAST (CHARG AS STRING) AS batch_number,
            CAST (MAKTX AS STRING) AS material_description,
            CAST (MTART AS STRING) AS material_type,
            CAST (MATKL AS STRING) AS material_group,
            CAST (BKLAS AS STRING) AS valuation_class,
            CAST (ZSATUAN AS STRING) AS unit_of_measure,
            CAST (QAWAL AS NUMERIC) AS initial_stock,
            CAST (NAWAL AS NUMERIC) AS received_quantity,
            CAST (QTERIMA AS NUMERIC) AS total_received_quantity,
            CAST (NTERIMA AS NUMERIC) AS total_received_amount,
            CAST (QKELUAR AS NUMERIC) AS quantity_out,
            CAST (NKELUAR AS NUMERIC) AS total_amount_out,
            CAST (QAKHIR AS NUMERIC) AS ending_stock,
            CAST (NAKHIR AS NUMERIC) AS ending_amount,
            CAST (ZSTAT AS STRING) AS status,
            CAST (ZYEAR AS STRING) AS year,
            CAST (last_update AS DATETIME) AS last_update,
            DATETIME('{current_ts}') AS last_insert_to_bigquery
        FROM bq_landing_zone.{SCHEMA}_{TABLE};
        """

    # Execute pakai BigQueryInsertJobOperator
    BigQueryInsertJobOperator(
        task_id='create_table',
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
    'dashboard_material-atpps',
    description="Doing incremental load from PostgreSQL to GCS",
    start_date=pendulum.datetime(2024, 9, 30, tz="Asia/Jakarta"),
    schedule_interval='0 3 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['Gen-AI', 'dashboard_material', 'refined'],
) as dag:

    dump_from_postgres_to_gcs = PostgresToGCSOperator(
        task_id='dump_from_postgres_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql=f"""
        SELECT
            "MANDT"::TEXT as "MANDT",
            "MATNR"::TEXT as "MATNR",
            "WERKS"::TEXT as "WERKS",
            "LGORT"::TEXT as "LGORT",
            "CHARG"::TEXT as "CHARG",
            "MAKTX"::TEXT as "MAKTX",
            "MTART"::TEXT as "MTART",
            "MATKL"::TEXT as "MATKL",
            "BKLAS"::TEXT as "BKLAS",
            "ZSATUAN"::TEXT as "ZSATUAN",
            "QAWAL"::NUMERIC as "QAWAL",
            "NAWAL"::NUMERIC as "NAWAL",
            "QTERIMA"::NUMERIC as "QTERIMA",
            "NTERIMA"::NUMERIC as "NTERIMA",
            "QKELUAR"::NUMERIC as "QKELUAR",
            "NKELUAR"::NUMERIC as "NKELUAR",
            "QAKHIR"::NUMERIC as "QAKHIR",
            "NAKHIR"::NUMERIC as "NAKHIR",
            "ZSTAT"::TEXT as "ZSTAT",
            "ZYEAR"::TEXT as "ZYEAR",
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
