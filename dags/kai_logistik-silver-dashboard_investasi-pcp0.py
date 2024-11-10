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
TABLE = 'pcp0'
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
            CAST(clnt as STRING) as clnt,
            CAST(docnum as STRING) as docnum,
            CAST(pernr as STRING) as pernr,
            CAST(NAME as STRING) as NAME,
            CAST(persa as STRING) as persa,
            CAST(PLANS as STRING) as PLANS,
            CAST(runid as STRING) as runid,
            CAST(tslin as STRING) as tslin,
            CAST(komok as STRING) as komok,
            CAST(rtline as STRING) as rtline,
            CAST(lgart as STRING) as lgart,
            CAST(abper as STRING) as abper,
            CAST(betrg as NUMERIC) as betrg,
            CAST(abkrs as STRING) as abkrs,
            CAST(persa_text as STRING) as persa_text,
            CAST(plans_text as STRING) as plans_text,
            CAST(lgart_text as STRING) as lgart_text,
            CAST(abkrs_text as STRING) as abkrs_text,
            CAST(doc_status as STRING) as doc_status,
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
    'dashboard_investasi-pcp0',
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
            "clnt"::TEXT as "clnt",
            "docnum"::TEXT as "docnum",
            "pernr"::TEXT as "pernr",
            "NAME"::TEXT as "NAME",
            "persa"::TEXT as "persa",
            "PLANS"::TEXT as "PLANS",
            "runid"::TEXT as "runid",
            "tslin"::TEXT as "tslin",
            "komok"::TEXT as "komok",
            "rtline"::TEXT as "rtline",
            "lgart"::TEXT as "lgart",
            "abper"::TEXT as "abper",
            "betrg"::NUMERIC as "betrg",
            "abkrs"::TEXT as "abkrs",
            "persa_text"::TEXT as "persa_text",
            "plans_text"::TEXT as "plans_text",
            "lgart_text"::TEXT as "lgart_text",
            "abkrs_text"::TEXT as "abkrs_text",
            "doc_status"::TEXT as "doc_status"
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
