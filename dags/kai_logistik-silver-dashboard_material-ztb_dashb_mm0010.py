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
TABLE = 'ztb_dashb_mm0010'
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
            CAST(MANDT AS STRING) AS MANDT,
            CAST(MATERIAL AS STRING) AS MATERIAL,
            CAST(PLANT AS STRING) AS PLANT,
            CAST(SLOC AS STRING) AS SLOC,
            CAST(POST_DATE AS STRING) AS POST_DATE,
            CAST(MAT_TYPE AS STRING) AS MAT_TYPE,
            CAST(MAT_GRP AS STRING) AS MAT_GRP,
            CAST(DESCRIPTION AS STRING) AS DESCRIPTION,
            CAST(QTY_ISSUE AS NUMERIC) AS QTY_ISSUE,
            CAST(QTY_NOW AS NUMERIC) AS QTY_NOW,
            CAST(SATUAN AS STRING) AS SATUAN,
            CAST(NILAI_ISSUE AS NUMERIC) AS NILAI_ISSUE,
            CAST(NILAI_NOW AS NUMERIC) AS NILAI_NOW,
            CAST(DOC_DATE AS STRING) AS DOC_DATE,
            CAST(ENTRY_DATE AS STRING) AS ENTRY_DATE,
            CAST(DATE_INSERT AS STRING) AS DATE_INSERT,
            CAST(TIME_INSERT AS STRING) AS TIME_INSERT,
            CAST(USER_INSERT AS STRING) AS USER_INSERT,
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
    'dashboard_material-ztb_dashb_mm0010',
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
            "MANDT"::TEXT as "MANDT",
            "MATERIAL"::TEXT as "MATERIAL",
            "PLANT"::TEXT as "PLANT",
            "SLOC"::TEXT as "SLOC",
            "POST_DATE"::TEXT as "POST_DATE",
            "MAT_TYPE"::TEXT as "MAT_TYPE",
            "MAT_GRP"::TEXT as "MAT_GRP",
            "DESCRIPTION"::TEXT as "DESCRIPTION",
            "QTY_ISSUE"::NUMERIC as "QTY_ISSUE",
            "QTY_NOW"::NUMERIC as "QTY_NOW",
            "SATUAN"::TEXT as "SATUAN",
            "NILAI_ISSUE"::NUMERIC as "NILAI_ISSUE",
            "NILAI_NOW"::NUMERIC as "NILAI_NOW",
            "DOC_DATE"::TEXT as "DOC_DATE",
            "ENTRY_DATE"::TEXT as "ENTRY_DATE",
            "DATE_INSERT"::TEXT as "DATE_INSERT",
            "TIME_INSERT"::TEXT as "TIME_INSERT",
            "USER_INSERT"::TEXT as "USER_INSERT",
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
