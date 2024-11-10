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
TABLE = 'users'
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
            CAST(id as STRING) as id,
            CAST(access_token as STRING) as access_token,
            CAST(username as STRING) as username,
            CAST(name as STRING) as name,
            CAST(email as STRING) as email,
            CAST(email_verified_at as DATETIME) as email_verified_at,
            CAST(password as STRING) as password,
            CAST(pernr as INTEGER) as pernr,
            CAST(persa as INTEGER) as persa,
            CAST(persa_text as STRING) as persa_text,
            CAST(orgeh as INTEGER) as orgeh,
            CAST(org_text as STRING) as org_text,
            CAST(plans as INTEGER) as plans,
            CAST(pos_text as STRING) as pos_text,
            CAST(status as STRING) as status,
            CAST(text_bus_area as STRING) as text_bus_area,
            CAST(attribute as STRING) as attribute,
            CAST(remember_token as STRING) as remember_token,
            CAST(created_at as DATETIME) as created_at,
            CAST(updated_at as DATETIME) as updated_at,
            CAST(prev_plans as STRING) as prev_plans,
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
    'dashboard_investasi-users',
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
            "access_token"::TEXT as "access_token",
            "username"::TEXT as "username",
            "name"::TEXT as "name",
            "email"::TEXT as "email",
            "email_verified_at"::TEXT as "email_verified_at",
            "password"::TEXT as "password",
            "pernr"::INTEGER as "pernr",
            "persa"::INTEGER as "persa",
            "persa_text"::TEXT as "persa_text",
            "orgeh"::INTEGER as "orgeh",
            "org_text"::TEXT as "org_text",
            "plans"::INTEGER as "plans",
            "pos_text"::TEXT as "pos_text",
            "status"::TEXT as "status",
            "text_bus_area"::TEXT as "text_bus_area",
            "attribute"::TEXT as "attribute",
            "remember_token"::TEXT as "remember_token",
            "created_at"::TEXT as "created_at",
            "updated_at"::TEXT as "updated_at",
            "prev_plans"::TEXT as "prev_plans"
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
