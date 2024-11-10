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
TABLE = 'ztb_dashb_ps0003'
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
            CAST(mandt as STRING) as mandt,
            CAST(posnr as STRING) as posnr,
            CAST(zyear as STRING) as zyear,
            CAST(deskripsi as STRING) as deskripsi,
            CAST(stufe as INTEGER) as stufe,
            CAST(up as STRING) as up,
            CAST(down as STRING) as down,
            CAST(kode_wbs as STRING) as kode_wbs,
            CAST(total_rka as NUMERIC) as total_rka,
            CAST(revisi_rka as NUMERIC) as revisi_rka,
            CAST(total_budget as NUMERIC) as total_budget,
            CAST(pra_bid_rp as NUMERIC) as pra_bid_rp,
            CAST(pra_bid_per as INTEGER) as pra_bid_per,
            CAST(pra_bid_char as STRING) as pra_bid_char,
            CAST(pro_bid_rp as NUMERIC) as pro_bid_rp,
            CAST(pro_bid_per as INTEGER) as pro_bid_per,
            CAST(pro_bid_char as STRING) as pro_bid_char,
            CAST(snk_rp as NUMERIC) as snk_rp,
            CAST(snk_per as INTEGER) as snk_per,
            CAST(snk_char as STRING) as snk_char,
            CAST(fisik as INTEGER) as fisik,
            CAST(bast_rp as NUMERIC) as bast_rp,
            CAST(bast_per as INTEGER) as bast_per,
            CAST(bast_char as STRING) as bast_char,
            CAST(jp_rp as NUMERIC) as jp_rp,
            CAST(jp_per as INTEGER) as jp_per,
            CAST(jp_char as STRING) as jp_char,
            CAST(bp_rp as NUMERIC) as bp_rp,
            CAST(bp_per as INTEGER) as bp_per,
            CAST(bp_char as STRING) as bp_char,
            CAST(batal_rp as NUMERIC) as batal_rp,
            CAST(batal_per as INTEGER) as batal_per,
            CAST(batal_char as STRING) as batal_char,
            CAST(total_po as NUMERIC) as total_po,
            CAST(total_pa as NUMERIC) as total_pa,
            CAST(objnr as STRING) as objnr,
            CAST(currency as STRING) as currency,
            CAST(banfn_pra as STRING) as banfn_pra,
            CAST(banfn_pro as STRING) as banfn_pro,
            CAST(ebeln_snk as STRING) as ebeln_snk,
            CAST(lvc_nkey as STRING) as lvc_nkey,
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
    'dashboard_investasi-ztb_dashb_ps0003',
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
            "mandt"::TEXT as "mandt",
            "posnr"::TEXT as "posnr",
            "zyear"::TEXT as "zyear",
            "deskripsi"::TEXT as "deskripsi",
            "stufe"::INTEGER as "stufe",
            "up"::TEXT as "up",
            "down"::TEXT as "down",
            "kode_wbs"::TEXT as "kode_wbs",
            "total_rka"::NUMERIC as "total_rka",
            "revisi_rka"::NUMERIC as "revisi_rka",
            "total_budget"::NUMERIC as "total_budget",
            "pra_bid_rp"::NUMERIC as "pra_bid_rp",
            "pra_bid_per"::BIGINT as "pra_bid_per",
            "pra_bid_char"::TEXT as "pra_bid_char",
            "pro_bid_rp"::NUMERIC as "pro_bid_rp",
            "pro_bid_per"::BIGINT as "pro_bid_per",
            "pro_bid_char"::TEXT as "pro_bid_char",
            "snk_rp"::NUMERIC as "snk_rp",
            "snk_per"::BIGINT as "snk_per",
            "snk_char"::TEXT as "snk_char",
            "fisik"::NUMERIC as "fisik",
            "bast_rp"::NUMERIC as "bast_rp",
            "bast_per"::BIGINT as "bast_per",
            "bast_char"::TEXT as "bast_char",
            "jp_rp"::NUMERIC as "jp_rp",
            "jp_per"::NUMERIC as "jp_per",
            "jp_char"::TEXT as "jp_char",
            "bp_rp"::NUMERIC as "bp_rp",
            "bp_per"::BIGINT as "bp_per",
            "bp_char"::TEXT as "bp_char",
            "batal_rp"::NUMERIC as "batal_rp",
            "batal_per"::BIGINT as "batal_per",
            "batal_char"::TEXT as "batal_char",
            "total_po"::NUMERIC as "total_po",
            "total_pa"::NUMERIC as "total_pa",
            "objnr"::TEXT as "objnr",
            "currency"::TEXT as "currency",
            "banfn_pra"::TEXT as "banfn_pra",
            "banfn_pro"::TEXT as "banfn_pro",
            "ebeln_snk"::TEXT as "ebeln_snk",
            "lvc_nkey"::TEXT as "lvc_nkey",
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
