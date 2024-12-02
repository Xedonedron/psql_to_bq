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
TABLE = 'ZFI0003'
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
            CAST(MANDT as STRING) as MANDT,
            CAST(BUKRS as STRING) as BUKRS,
            CAST(BELNR as STRING) as BELNR,
            CAST(GJAHR as STRING) as GJAHR,
            CAST(APPRV as INTEGER) as APPRV,
            CAST(DMBTR as NUMERIC) as DMBTR,
            CAST(NAME1 as STRING) as NAME1,
            CAST(XBLNR as STRING) as XBLNR,
            CAST(PYORD as STRING) as PYORD,
            CAST(STATS as STRING) as STATS,
            CAST(FCTXT as STRING) as FCTXT,
            CAST(KOSTL as STRING) as KOSTL,
            CAST(BUDAT as STRING) as BUDAT,
            CAST(BSTAT as STRING) as BSTAT,
            CAST(WAERS as STRING) as WAERS,
            CAST(KNAME as STRING) as KNAME,
            CAST(DOCA8 as STRING) as DOCA8,
            CAST(AUGBL as STRING) as AUGBL,
            CAST(UMSKZ as STRING) as UMSKZ,
            CAST(GSBER as STRING) as GSBER,
            CAST(XBDAT as STRING) as XBDAT,
            CAST(PYDAT as STRING) as PYDAT,
            CAST(BSDAT as STRING) as BSDAT,
            CAST(BKTXT as STRING) as BKTXT,
            CAST(FISTL as STRING) as FISTL,
            CAST(LIFNR as STRING) as LIFNR,
            CAST(UMDID as STRING) as UMDID,
            CAST(last_update as DATETIME) as last_update,
            CAST(FIPOS as STRING) as FIPOS,
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
    'dashboard_investasi-ZFI0003',
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
            "MANDT"::TEXT as "MANDT",
            "BUKRS"::TEXT as "BUKRS",
            "BELNR"::TEXT as "BELNR",
            "GJAHR"::TEXT as "GJAHR",
            "APPRV"::INTEGER as "APPRV",
            "DMBTR"::NUMERIC as "DMBTR",
            "NAME1"::TEXT as "NAME1",
            "XBLNR"::TEXT as "XBLNR",
            "PYORD"::TEXT as "PYORD",
            "STATS"::TEXT as "STATS",
            "FCTXT"::TEXT as "FCTXT",
            "KOSTL"::TEXT as "KOSTL",
            "BUDAT"::TEXT as "BUDAT",
            "BSTAT"::TEXT as "BSTAT",
            "WAERS"::TEXT as "WAERS",
            "KNAME"::TEXT as "KNAME",
            "DOCA8"::TEXT as "DOCA8",
            "AUGBL"::TEXT as "AUGBL",
            "UMSKZ"::TEXT as "UMSKZ",
            "GSBER"::TEXT as "GSBER",
            "XBDAT"::TEXT as "XBDAT",
            "PYDAT"::TEXT as "PYDAT",
            "BSDAT"::TEXT as "BSDAT",
            "BKTXT"::TEXT as "BKTXT",
            "FISTL"::TEXT as "FISTL",
            "LIFNR"::TEXT as "LIFNR",
            "UMDID"::TEXT as "UMDID",
            "last_update"::TEXT as "last_update",
            "FIPOS"::TEXT as "FIPOS"
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
