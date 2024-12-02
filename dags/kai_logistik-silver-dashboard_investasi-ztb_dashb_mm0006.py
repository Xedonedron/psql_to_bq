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
TABLE = 'ztb_dashb_mm0006'
ALTER_TABLE = "data_po_invoice"
GCS_BUCKET = 'kai_sap'
FILE_NAME = f'{TABLE}.parquet_{{}}'

def push_current_timestamp():
    jakarta_tz = pytz.timezone('Asia/Jakarta')
    current_ts = datetime.now(jakarta_tz).strftime('%Y-%m-%d %H:%M:%S')
    return current_ts  # Push to XCom automatically


def build_query_and_run(**kwargs):
    ti = kwargs['ti']
    current_ts = ti.xcom_pull(task_ids='push_ts_task')

    query = f"""
        INSERT INTO {PROJECT_ID}.{SCHEMA}.{ALTER_TABLE} (cl, purch_doc, item, doc_no, year, invitem, material_document_numer, service_entry_sheet_numer, matyr, item_2, documentno, doc_type, doc_date, pstng_date, entry_date, time, reference, document_header_text, plnt, amount, crcy, d_c, vendor, cocd, a, i, last_update, last_insert_to_bigquery)
        SELECT
            CAST(MANDT as STRING) as cl,
            CAST(EBELN as STRING) as purch_doc,
            CAST(EBELP as STRING) as item,
            CAST(BELNR as STRING) as doc_no,
            CAST(GJAHR as STRING) as year,
            CAST(BUZEI as STRING) as invitem,
            CAST(LFBNR as STRING) as material_document_numer,
            CAST(LBLNI as STRING) as service_entry_sheet_numer,
            CAST(LFGJA as STRING) as matyr,
            CAST(LFPOS as STRING) as item_2,
            CAST(ACC_DOC as STRING) as documentno,
            CAST(BLART as STRING) as doc_type,
            CAST(BLDAT as STRING) as doc_date,
            CAST(BUDAT as STRING) as pstng_date,
            CAST(CPUDT as STRING) as entry_date,
            CAST(CPUTM as STRING) as time,
            CAST(XBLNR as STRING) as reference,
            CAST(BKTXT as STRING) as document_header_text,
            CAST(WERKS as STRING) as plnt,
            CAST(WRBTR as NUMERIC) as amount,
            CAST(WAERS as STRING) as crcy,
            CAST(SHKZG as STRING) as d_c,
            CAST(LIFNR as STRING) as vendor,
            CAST(BUKRS as STRING) as cocd,
            CAST(KNTTP as STRING) as a,
            CAST(PSTYP as STRING) as i,
            CAST(LAST_UPDATE as DATETIME) as last_update,
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
    'dashboard_investasi-ztb_dashb_mm0006',
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
            "MANDT"::TEXT as "MANDT",
            "EBELN"::TEXT as "EBELN",
            "EBELP"::TEXT as "EBELP",
            "BELNR"::TEXT as "BELNR",
            "GJAHR"::TEXT as "GJAHR",
            "BUZEI"::TEXT as "BUZEI",
            "LFBNR"::TEXT as "LFBNR",
            "LBLNI"::TEXT as "LBLNI",
            "LFGJA"::TEXT as "LFGJA",
            "LFPOS"::TEXT as "LFPOS",
            "ACC_DOC"::TEXT as "ACC_DOC",
            "BLART"::TEXT as "BLART",
            "BLDAT"::TEXT as "BLDAT",
            "BUDAT"::TEXT as "BUDAT",
            "CPUDT"::TEXT as "CPUDT",
            "CPUTM"::TEXT as "CPUTM",
            "XBLNR"::TEXT as "XBLNR",
            "BKTXT"::TEXT as "BKTXT",
            "WERKS"::TEXT as "WERKS",
            "WRBTR"::NUMERIC as "WRBTR",
            "WAERS"::TEXT as "WAERS",
            "SHKZG"::TEXT as "SHKZG",
            "LIFNR"::TEXT as "LIFNR",
            "BUKRS"::TEXT as "BUKRS",
            "KNTTP"::TEXT as "KNTTP",
            "PSTYP"::TEXT as "PSTYP",
            "LAST_UPDATE"::TEXT as "LAST_UPDATE"
        FROM {SCHEMA}."{TABLE}";
        """,
        bucket=GCS_BUCKET,
        filename=f'{SCHEMA}/{TABLE}/{FILE_NAME}',  # Nama file yang disimpan di GCS
        export_format='parquet',
        gcp_conn_id='kai_genai_prod',
        approx_max_file_size_bytes=20 * 1024 * 1024, # split 20 MB
        # parquet_row_group_size=1000000,
    )

    load_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=GCS_BUCKET,
        source_objects=f'{SCHEMA}/{TABLE}/*',
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
