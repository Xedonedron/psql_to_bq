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
TABLE = 'test_bulk'
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
            CAST(Column0 as INTEGER) as Column0,
            CAST(Column1 as INTEGER) as Column1,
            CAST(Column2 as INTEGER) as Column2,
            CAST(Column3 as STRING) as Column3,
            CAST(Column4 as INTEGER) as Column4,
            CAST(Column5 as INTEGER) as Column5,
            CAST(Column6 as STRING) as Column6,
            CAST(Column7 as STRING) as Column7,
            CAST(Column8 as STRING) as Column8,
            CAST(Column9 as NUMERIC) as Column9,
            CAST(Column10 as STRING) as Column10,
            CAST(Column11 as NUMERIC) as Column11,
            CAST(Column12 as STRING) as Column12,
            CAST(Column13 as STRING) as Column13,
            CAST(Column14 as STRING) as Column14,
            CAST(Column15 as STRING) as Column15,
            CAST(Column16 as STRING) as Column16,
            CAST(Column17 as INTEGER) as Column17,
            CAST(Column18 as STRING) as Column18,
            CAST(Column19 as STRING) as Column19,
            CAST(Column20 as STRING) as Column20,
            CAST(Column21 as INTEGER) as Column21,
            CAST(Column22 as INTEGER) as Column22,
            CAST(Column23 as STRING) as Column23,
            CAST(Column24 as STRING) as Column24,
            CAST(Column25 as STRING) as Column25,
            CAST(Column26 as STRING) as Column26,
            CAST(Column27 as STRING) as Column27,
            CAST(Column28 as STRING) as Column28,
            CAST(Column29 as INTEGER) as Column29,
            CAST(Column30 as INTEGER) as Column30,
            CAST(Column31 as STRING) as Column31,
            CAST(Column32 as STRING) as Column32,
            CAST(Column33 as STRING) as Column33,
            CAST(Column34 as STRING) as Column34,
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
    'dashboard_investasi-test_bulk',
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
            "Column0"::INTEGER as "Column0",
            "Column1"::BIGINT as "Column1",
            "Column2"::INTEGER as "Column2",
            "Column3"::TEXT as "Column3",
            "Column4"::INTEGER as "Column4",
            "Column5"::BIGINT as "Column5",
            "Column6"::TEXT as "Column6",
            "Column7"::TEXT as "Column7",
            "Column8"::TEXT as "Column8",
            "Column9"::NUMERIC as "Column9",
            "Column10"::TEXT as "Column10",
            "Column11"::NUMERIC as "Column11",
            "Column12"::TEXT as "Column12",
            "Column13"::TEXT as "Column13",
            "Column14"::TEXT as "Column14",
            "Column15"::TEXT as "Column15",
            "Column16"::TEXT as "Column16",
            "Column17"::BIGINT as "Column17",
            "Column18"::TEXT as "Column18",
            "Column19"::TEXT as "Column19",
            "Column20"::TEXT as "Column20",
            "Column21"::INTEGER as "Column21",
            "Column22"::BIGINT as "Column22",
            "Column23"::TEXT as "Column23",
            "Column24"::TEXT as "Column24",
            "Column25"::TEXT as "Column25",
            "Column26"::TEXT as "Column26",
            "Column27"::TEXT as "Column27",
            "Column28"::TEXT as "Column28",
            "Column29"::INTEGER as "Column29",
            "Column30"::INTEGER as "Column30",
            "Column31"::TEXT as "Column31",
            "Column32"::TEXT as "Column32",
            "Column33"::TEXT as "Column33",
            "Column34"::TEXT as "Column34"
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
