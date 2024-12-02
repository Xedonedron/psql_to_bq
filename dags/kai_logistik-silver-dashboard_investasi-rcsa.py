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
TABLE = 'rcsa'
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
            SAFE_CAST(id as INTEGER) as id,
            SAFE_CAST(code as STRING) as code,
            SAFE_CAST(orgeh as INTEGER) as orgeh,
            SAFE_CAST(position_id as INTEGER) as position_id,
            SAFE_CAST(pernr as INTEGER) as pernr,
            SAFE_CAST(period_id as INTEGER) as period_id,
            SAFE_CAST(period_year as INTEGER) as period_year,
            SAFE_CAST(name as STRING) as name,
            SAFE_CAST(incident_description as STRING) as incident_description,
            SAFE_CAST(risk_category as INTEGER) as risk_category,
            SAFE_CAST(risk_category_sub as INTEGER) as risk_category_sub,
            SAFE_CAST(inherent_possibility as INTEGER) as inherent_possibility,
            SAFE_CAST(inherent_impact as INTEGER) as inherent_impact,
            SAFE_CAST(inherent_value as INTEGER) as inherent_value,
            SAFE_CAST(inherent_weight as INTEGER) as inherent_weight,
            SAFE_CAST(inherent_top_risk as NUMERIC) as inherent_top_risk,
            SAFE_CAST(inherent_level as STRING) as inherent_level,
            SAFE_CAST(residual_possibility as INTEGER) as residual_possibility,
            SAFE_CAST(residual_impact as INTEGER) as residual_impact,
            SAFE_CAST(residual_value as INTEGER) as residual_value,
            SAFE_CAST(residual_level as STRING) as residual_level,
            SAFE_CAST(created_by as STRING) as created_by,
            SAFE_CAST(updated_by as STRING) as updated_by,
            SAFE_CAST(deleted_by as STRING) as deleted_by,
            SAFE_CAST(status as STRING) as status,
            SAFE_CAST(type as STRING) as type,
            SAFE_CAST(attribute as STRING) as attribute,
            SAFE_CAST(rcsa_configure_id as INTEGER) as rcsa_configure_id,
            SAFE_CAST(created_at as DATETIME) as created_at,
            SAFE_CAST(updated_at as DATETIME) as updated_at,
            SAFE_CAST(deleted_at as DATETIME) as deleted_at,
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
    'dashboard_investasi-rcsa',
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
            "id"::BIGINT as "id",
            "code"::TEXT as "code",
            "orgeh"::BIGINT as "orgeh",
            "position_id"::BIGINT as "position_id",
            "pernr"::BIGINT as "pernr",
            "period_id"::BIGINT as "period_id",
            "period_year"::BIGINT as "period_year",
            "name"::TEXT as "name",
            "incident_description"::TEXT as "incident_description",
            "risk_category"::BIGINT as "risk_category",
            "risk_category_sub"::BIGINT as "risk_category_sub",
            "inherent_possibility"::BIGINT as "inherent_possibility",
            "inherent_impact"::BIGINT as "inherent_impact",
            "inherent_value"::BIGINT as "inherent_value",
            "inherent_weight"::BIGINT as "inherent_weight",
            "inherent_top_risk"::DOUBLE PRECISION as "inherent_top_risk",
            "inherent_level"::TEXT as "inherent_level",
            "residual_possibility"::BIGINT as "residual_possibility",
            "residual_impact"::BIGINT as "residual_impact",
            "residual_value"::BIGINT as "residual_value",
            "residual_level"::TEXT as "residual_level",
            "created_by"::TEXT as "created_by",
            "updated_by"::TEXT as "updated_by",
            "deleted_by"::TEXT as "deleted_by",
            "status"::TEXT as "status",
            "type"::TEXT as "type",
            "attribute"::TEXT as "attribute",
            "rcsa_configure_id"::BIGINT as "rcsa_configure_id",
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
