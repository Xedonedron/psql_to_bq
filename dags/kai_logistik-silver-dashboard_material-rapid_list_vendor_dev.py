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
TABLE = 'rapid_list_vendor_dev'
GCS_BUCKET = 'kai_smartsheet'
FILE_NAME = f'{TABLE}.parquet_{{}}'

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
            CAST(V_NM as STRING) as V_NM,
            CAST(BID_USH_NM as STRING) as BID_USH_NM,
            CAST(SB_BID_USH_NM as STRING) as SB_BID_USH_NM,
            CAST(V_TGL_BUAT as DATETIME) as V_TGL_BUAT,
            CAST(V_PNG_JWB as STRING) as V_PNG_JWB,
            CAST(V_PNG_JWB_EMAIL as STRING) as V_PNG_JWB_EMAIL,
            CAST(V_PNG_JWB_HP as STRING) as V_PNG_JWB_HP,
            CAST(V_DRKTR as STRING) as V_DRKTR,
            CAST(V_DRKTR_EMAIL as STRING) as V_DRKTR_EMAIL,
            CAST(V_DRKTR_HP as STRING) as V_DRKTR_HP,
            CAST(V_FINANCE_NM as STRING) as V_FINANCE_NM,
            CAST(V_FINANCE_EMAIL as STRING) as V_FINANCE_EMAIL,
            CAST(V_FINANCE_HP as STRING) as V_FINANCE_HP,
            CAST(TANGGAL_AKTIF as DATETIME) as TANGGAL_AKTIF,
            CAST(STATUS_NM as STRING) as STATUS_NM,
            CAST(GOLONGAN as STRING) as GOLONGAN,
            CAST(V_ALMT1 as STRING) as V_ALMT1,
            CAST(V_CITY as STRING) as V_CITY,
            CAST(NEGARA_NM as STRING) as NEGARA_NM,
            CAST(V_KD_POS as STRING) as V_KD_POS,
            CAST(PROV_NM as STRING) as PROV_NM,
            CAST(V_NPWP as STRING) as V_NPWP,
            CAST(V_KTP as STRING) as V_KTP,
            CAST(V_TELP as STRING) as V_TELP,
            CAST(V_FAX as STRING) as V_FAX,
            CAST(V_MERK_DAGANG as STRING) as V_MERK_DAGANG,
            CAST(V_HOMEPAGE as STRING) as V_HOMEPAGE,
            CAST(V_NOTIFICATION_EMAIL as STRING) as V_NOTIFICATION_EMAIL,
            CAST(V_SALES_NM as STRING) as V_SALES_NM,
            CAST(V_SALES_EMAIL as STRING) as V_SALES_EMAIL,
            CAST(V_SALES_HP as STRING) as V_SALES_HP,
            CAST(KUAL_NM as STRING) as KUAL_NM,
            CAST(VC_NO as STRING) as VC_NO,
            CAST(VCS_NAME as STRING) as VCS_NAME,
            CAST(KTGRV_KET as STRING) as KTGRV_KET,
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
    'dashboard_material-rapid_list_vendor_dev',
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
            "V_NM"::TEXT as "V_NM",
            "BID_USH_NM"::TEXT as "BID_USH_NM",
            "SB_BID_USH_NM"::TEXT as "SB_BID_USH_NM",
            "V_TGL_BUAT"::TEXT as "V_TGL_BUAT",
            "V_PNG_JWB"::TEXT as "V_PNG_JWB",
            "V_PNG_JWB_EMAIL"::TEXT as "V_PNG_JWB_EMAIL",
            "V_PNG_JWB_HP"::TEXT as "V_PNG_JWB_HP",
            "V_DRKTR"::TEXT as "V_DRKTR",
            "V_DRKTR_EMAIL"::TEXT as "V_DRKTR_EMAIL",
            "V_DRKTR_HP"::TEXT as "V_DRKTR_HP",
            "V_FINANCE_NM"::TEXT as "V_FINANCE_NM",
            "V_FINANCE_EMAIL"::TEXT as "V_FINANCE_EMAIL",
            "V_FINANCE_HP"::TEXT as "V_FINANCE_HP",
            "TANGGAL_AKTIF"::TEXT as "TANGGAL_AKTIF",
            "STATUS_NM"::TEXT as "STATUS_NM",
            "GOLONGAN"::TEXT as "GOLONGAN",
            "V_ALMT1"::TEXT as "V_ALMT1",
            "V_CITY"::TEXT as "V_CITY",
            "NEGARA_NM"::TEXT as "NEGARA_NM",
            "V_KD_POS"::TEXT as "V_KD_POS",
            "PROV_NM"::TEXT as "PROV_NM",
            "V_NPWP"::TEXT as "V_NPWP",
            "V_KTP"::TEXT as "V_KTP",
            "V_TELP"::TEXT as "V_TELP",
            "V_FAX"::TEXT as "V_FAX",
            "V_MERK_DAGANG"::TEXT as "V_MERK_DAGANG",
            "V_HOMEPAGE"::TEXT as "V_HOMEPAGE",
            "V_NOTIFICATION_EMAIL"::TEXT as "V_NOTIFICATION_EMAIL",
            "V_SALES_NM"::TEXT as "V_SALES_NM",
            "V_SALES_EMAIL"::TEXT as "V_SALES_EMAIL",
            "V_SALES_HP"::TEXT as "V_SALES_HP",
            "KUAL_NM"::TEXT as "KUAL_NM",
            "VC_NO"::TEXT as "VC_NO",
            "VCS_NAME"::TEXT as "VCS_NAME",
            "KTGRV_KET"::TEXT as "KTGRV_KET"
        FROM {SCHEMA}.{TABLE};
        """,
        bucket=GCS_BUCKET,
        filename=f'{SCHEMA}/{TABLE}/{FILE_NAME}',  # Nama file yang disimpan di GCS
        export_format='parquet',
        gcp_conn_id='kai_genai_prod',
        approx_max_file_size_bytes=20 * 1024 * 1024, # split 20 MB
        parquet_row_group_size=1000000,
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
