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
TABLE = 'cdm_kramasan'
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
        CREATE or REPLACE TABLE {PROJECT_ID}.{SCHEMA}.{TABLE} as
        SELECT
            CAST(id as INTEGER) as id,
            CAST(ba as STRING) as ba,
            CAST(ba_latitude as NUMERIC) as ba_latitude,
            CAST(ba_longitude as NUMERIC) as ba_longitude,
            CAST(business_area as STRING) as business_area,
            CAST(amount_kontrak as NUMERIC) as amount_kontrak,
            CAST(tanggal_akhir_kontrak as DATETIME) as tanggal_akhir_kontrak,
            CAST(tanggal_awal_kontrak as DATETIME) as tanggal_awal_kontrak,
            CAST(delay_text as STRING) as delay_text,
            CAST(kategori_investasi as STRING) as kategori_investasi,
            CAST(objnr as STRING) as objnr,
            CAST(keterangan_pb as STRING) as keterangan_pb,
            CAST(keterangan_pf as STRING) as keterangan_pf,
            CAST(posid as STRING) as posid,
            CAST(posid_1 as STRING) as posid_1,
            CAST(posid_2 as STRING) as posid_2,
            CAST(posid_3 as STRING) as posid_3,
            CAST(post1 as STRING) as post1,
            CAST(post1_1 as STRING) as post1_1,
            CAST(post1_2 as STRING) as post1_2,
            CAST(post1_3 as STRING) as post1_3,
            CAST(progress_budget as NUMERIC) as progress_budget,
            CAST(tanggal as DATETIME) as tanggal,
            CAST(progress_fisik as NUMERIC) as progress_fisik,
            CAST(amount_real as NUMERIC) as amount_real,
            CAST(nama_vendor as STRING) as nama_vendor,
            CAST(wbs_level as INTEGER) as wbs_level,
            CAST(zbudgtotal as NUMERIC) as zbudgtotal,
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
    'dashboard_investasi-cdm_kramasan',
    description="Doing incremental load from PostgreSQL to GCS",
    start_date=pendulum.datetime(2024, 9, 30, tz="Asia/Jakarta"),
    schedule_interval='* 1 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['Gen-AI', 'dashboard_investasi', 'refined'],
) as dag:

    dump_from_postgres_to_gcs = PostgresToGCSOperator(
        task_id='dump_from_postgres_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql=f"""
        SELECT
            "id"::TEXT as "id",
            "ba"::TEXT as "ba",
            "ba_latitude"::NUMERIC as "ba_latitude",
            "ba_longitude"::NUMERIC as "ba_longitude",
            "business_area"::TEXT as "business_area",
            "amount_kontrak"::NUMERIC as "amount_kontrak",
            "tanggal_akhir_kontrak"::TEXT as "tanggal_akhir_kontrak",
            "tanggal_awal_kontrak"::TEXT as "tanggal_awal_kontrak",
            "delay_text"::TEXT as "delay_text",
            "kategori_investasi"::TEXT as "kategori_investasi",
            "objnr"::TEXT as "objnr",
            "keterangan_pb"::TEXT as "keterangan_pb",
            "keterangan_pf"::TEXT as "keterangan_pf",
            "posid"::TEXT as "posid",
            "posid_1"::TEXT as "posid_1",
            "posid_2"::TEXT as "posid_2",
            "posid_3"::TEXT as "posid_3",
            "post1"::TEXT as "post1",
            "post1_1"::TEXT as "post1_1",
            "post1_2"::TEXT as "post1_2",
            "post1_3"::TEXT as "post1_3",
            "progress_budget"::NUMERIC as "progress_budget",
            "tanggal"::TEXT as "tanggal",
            "progress_fisik"::NUMERIC as "progress_fisik",
            "amount_real"::NUMERIC as "amount_real",
            "nama_vendor"::TEXT as "nama_vendor",
            "wbs_level"::TEXT as "wbs_level",
            "zbudgtotal"::NUMERIC as "zbudgtotal"
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
