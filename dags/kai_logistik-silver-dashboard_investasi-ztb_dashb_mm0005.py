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
TABLE = 'ztb_dashb_mm0005'
ALTER_TABLE = "data_header_pengadaan"
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
        INSERT INTO {PROJECT_ID}.{SCHEMA}.{ALTER_TABLE} (purch_req, item, purch_doc, item_2, header_text, attachment, in_tem_of, `to`, requistioner, nomor_kontrak, plnt, req_date, vp_start, created_by, last_update, last_insert_to_bigquer)
        SELECT
            CAST(banfn as STRING) as purch_req,
            CAST(bnfpo as STRING) as item,
            CAST(ebeln as STRING) as purch_doc,
            CAST(ebelp as STRING) as item_2,
            CAST(zhtext as STRING) as header_text,
            CAST(zattach as STRING) as attachment,
            CAST(zinterm as STRING) as in_tem_of,
            CAST(zto as STRING) as `to`,
            CAST(zrequis as STRING) as requistioner,
            CAST(zkontrak as STRING) as nomor_kontrak,
            CAST(zplant as STRING) as plnt,
            CAST(zdate as STRING) as req_date,
            CAST(kdatb as STRING) as vp_start,
            CAST(ernam as STRING) as created_by,
            CAST(last_update as DATETIME) as last_update,
            DATETIME('{current_ts}') AS last_insert_to_bigquery
        FROM {PROJECT_ID}.bq_landing_zone.{SCHEMA}_{TABLE};
    """

    # Execute pakai BigQueryInsertJobOperator
    BigQueryInsertJobOperator(
        task_id='load_to_refined_zone',
        location='asia-southeast2',
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
    'dashboard_investasi-ztb_dashb_mm0005',
    description="Doing incremental load from PostgreSQL to GCS",
    start_date=pendulum.datetime(2024, 9, 30, tz="Asia/Jakarta"),
    schedule_interval='0 2 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['Gen-AI', 'dashboard_investasi', 'refined'],
) as dag:

    dump_from_postgres_to_gcs = PostgresToGCSOperator(
        task_id='dump_from_postgres_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql=f"""
        SELECT
            "banfn"::TEXT as "banfn",
            "bnfpo"::TEXT as "bnfpo",
            "ebeln"::TEXT as "ebeln",
            "ebelp"::TEXT as "ebelp",
            "zhtext"::TEXT as "zhtext",
            "zattach"::TEXT as "zattach",
            "zinterm"::TEXT as "zinterm",
            "zto"::TEXT as "zto",
            "zrequis"::TEXT as "zrequis",
            "zkontrak"::TEXT as "zkontrak",
            "zplant"::TEXT as "zplant",
            "zdate"::TEXT as "zdate",
            "kdatb"::TEXT as "kdatb",
            "ernam"::TEXT as "ernam",
            "last_update"::TEXT as "last_update"
        FROM {SCHEMA}."{TABLE}";
        """,
        bucket=GCS_BUCKET,
        filename=f'{SCHEMA}/{TABLE}/{FILE_NAME}',  # Nama file yang disimpan di GCS
        export_format='parquet',
        gcp_conn_id='kai_genai_prod',
        approx_max_file_size_bytes=50 * 1024 * 1024, # split 50 MB
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
