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
TABLE = 'rcsa_configure'
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
            CAST(description as STRING) as description,
            CAST(persa as INTEGER) as persa,
            CAST(unit_id as INTEGER) as unit_id,
            CAST(unit_name as STRING) as unit_name,
            CAST(period_project as INTEGER) as period_project,
            CAST(object_assessment_id as INTEGER) as object_assessment_id,
            CAST(object_assessment_name as STRING) as object_assessment_name,
            CAST(risk_head_id as INTEGER) as risk_head_id,
            CAST(risk_head_name as STRING) as risk_head_name,
            CAST(risk_analyst_id as INTEGER) as risk_analyst_id,
            CAST(risk_analyst_name as STRING) as risk_analyst_name,
            CAST(risk_vp_id as INTEGER) as risk_vp_id,
            CAST(risk_vp_name as STRING) as risk_vp_name,
            CAST(project_currency as STRING) as project_currency,
            CAST(project_amount as NUMERIC) as project_amount,
            CAST(project_initiator_id as INTEGER) as project_initiator_id,
            CAST(project_initiator_name as STRING) as project_initiator_name,
            CAST(status as STRING) as status,
            CAST(created_at as DATETIME) as created_at,
            CAST(updated_at as DATETIME) as updated_at,
            CAST(deleted_at as DATETIME) as deleted_at,
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
    'dashboard_investasi-rcsa_configure',
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
            "id"::BIGINT as "id",
            "description"::TEXT as "description",
            "persa"::INTEGER as "persa",
            "unit_id"::INTEGER as "unit_id",
            "unit_name"::TEXT as "unit_name",
            "period_project"::INTEGER as "period_project",
            "object_assessment_id"::INTEGER as "object_assessment_id",
            "object_assessment_name"::TEXT as "object_assessment_name",
            "risk_head_id"::INTEGER as "risk_head_id",
            "risk_head_name"::TEXT as "risk_head_name",
            "risk_analyst_id"::INTEGER as "risk_analyst_id",
            "risk_analyst_name"::TEXT as "risk_analyst_name",
            "risk_vp_id"::INTEGER as "risk_vp_id",
            "risk_vp_name"::TEXT as "risk_vp_name",
            "project_currency"::TEXT as "project_currency",
            "project_amount"::NUMERIC as "project_amount",
            "project_initiator_id"::INTEGER as "project_initiator_id",
            "project_initiator_name"::TEXT as "project_initiator_name",
            "status"::TEXT as "status",
            "created_at"::TEXT as "created_at",
            "updated_at"::TEXT as "updated_at",
            "deleted_at"::TEXT as "deleted_at"
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
