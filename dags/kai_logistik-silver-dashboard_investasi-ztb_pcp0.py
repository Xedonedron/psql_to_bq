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
TABLE = 'ztb_pcp0'
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
            CAST(hkont as STRING) as hkont,
            CAST(gsber as STRING) as gsber,
            CAST(docnum as STRING) as docnum,
            CAST(pernr as STRING) as pernr,
            CAST(persk as STRING) as persk,
            CAST(persa as STRING) as persa,
            CAST(PLANS as STRING) as PLANS,
            CAST(komok as STRING) as komok,
            CAST(abper as STRING) as abper,
            CAST(abkrs as STRING) as abkrs,
            CAST(tslin as STRING) as tslin,
            CAST(rtline as STRING) as rtline,
            CAST(lgart as STRING) as lgart,
            CAST(betrg as NUMERIC) as betrg,
            CAST(runid as STRING) as runid,
            CAST(name as STRING) as name,
            CAST(trfgr as STRING) as trfgr,
            CAST(waers as STRING) as waers,
            CAST(stell as STRING) as stell,
            CAST(persg as STRING) as persg,
            CAST(amount_type as STRING) as amount_type,
            CAST(kostl as STRING) as kostl,
            CAST(persa_text as STRING) as persa_text,
            CAST(plans_text as STRING) as plans_text,
            CAST(lgart_text as STRING) as lgart_text,
            CAST(abkrs_text as STRING) as abkrs_text,
            CAST(doc_status as STRING) as doc_status,
            CAST(gsber_text as STRING) as gsber_text,
            CAST(stell_text as STRING) as stell_text,
            CAST(persk_text as STRING) as persk_text,
            CAST(persg_text as STRING) as persg_text,
            CAST(org_unit as STRING) as org_unit,
            CAST(org_unit_text as STRING) as org_unit_text,
            CAST(gol as STRING) as gol,
            CAST(ruang as STRING) as ruang,
            CAST(imo as STRING) as imo,
            CAST(sgtxt as STRING) as sgtxt,
            CAST(user_insert as STRING) as user_insert,
            CAST(date_insert as STRING) as date_insert,
            CAST(last_update as DATETIME) as last_update,
            DATETIME('{current_ts}') AS last_insert_to_bigquery
        FROM {PROJECT_ID}.bq_landing_zone.{SCHEMA}_{TABLE};
    """

    # Execute pakai BigQueryInsertJobOperator
    BigQueryInsertJobOperator(
        task_id='load_to_refined_zone',
        location='asia-southeast2',
        gcp_conn_id='kai_genai_prod',
        project_id = 'kai-genai-prod',
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False, 
            }
        },
        dag=kwargs['dag']
    ).execute(context=kwargs)
    
with models.DAG(
    'dashboard_investasi-ztb_pcp0',
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
            "hkont"::TEXT as "hkont",
            "gsber"::TEXT as "gsber",
            "docnum"::TEXT as "docnum",
            "pernr"::TEXT as "pernr",
            "persk"::TEXT as "persk",
            "persa"::TEXT as "persa",
            "PLANS"::TEXT as "PLANS",
            "komok"::TEXT as "komok",
            "abper"::TEXT as "abper",
            "abkrs"::TEXT as "abkrs",
            "tslin"::TEXT as "tslin",
            "rtline"::TEXT as "rtline",
            "lgart"::TEXT as "lgart",
            "betrg"::NUMERIC as "betrg",
            "runid"::TEXT as "runid",
            "name"::TEXT as "name",
            "trfgr"::TEXT as "trfgr",
            "waers"::TEXT as "waers",
            "stell"::TEXT as "stell",
            "persg"::TEXT as "persg",
            "amount_type"::TEXT as "amount_type",
            "kostl"::TEXT as "kostl",
            "persa_text"::TEXT as "persa_text",
            "plans_text"::TEXT as "plans_text",
            "lgart_text"::TEXT as "lgart_text",
            "abkrs_text"::TEXT as "abkrs_text",
            "doc_status"::TEXT as "doc_status",
            "gsber_text"::TEXT as "gsber_text",
            "stell_text"::TEXT as "stell_text",
            "persk_text"::TEXT as "persk_text",
            "persg_text"::TEXT as "persg_text",
            "org_unit"::TEXT as "org_unit",
            "org_unit_text"::TEXT as "org_unit_text",
            "gol"::TEXT as "gol",
            "ruang"::TEXT as "ruang",
            "imo"::TEXT as "imo",
            "sgtxt"::TEXT as "sgtxt",
            "user_insert"::TEXT as "user_insert",
            "date_insert"::TEXT as "date_insert",
            "last_update"::TEXT as "last_update"
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
