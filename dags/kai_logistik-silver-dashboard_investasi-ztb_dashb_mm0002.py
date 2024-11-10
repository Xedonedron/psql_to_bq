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
TABLE = 'ztb_dashb_mm0002'
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
        INSERT INTO {PROJECT_ID}.{SCHEMA}.data_account_assignment_pr (cl, purch_req, item, ser, get_data_year, d, busa, g_l_acct, cost_ctr, aufnr, asset, total_val_upon_release, funds_ctr, commitment_ltm, profit_ctr, prof_seg, nomor_produk_kai, name, wbs_element, wbs_element_2, last_update, last_insert_to_bigquer)
        SELECT
            CAST(mandt as STRING) as cl,
            CAST(banfn as STRING) as purch_req,
            CAST(bnfpo as STRING) as item,
            CAST(zebkn as STRING) as ser,
            CAST(zyear as STRING) as get_data_year,
            CAST(loekz as STRING) as d,
            CAST(gsber as STRING) as busa,
            CAST(sakto as STRING) as g_l_acct,
            CAST(kostl as STRING) as cost_ctr,
            CAST(aufnr as STRING) as aufnr,
            CAST(anln1 as STRING) as asset,
            CAST(netwr as NUMERIC) as total_val_upon_release,
            CAST(fistl as STRING) as funds_ctr,
            CAST(fipos as STRING) as commitment_ltm,
            CAST(prctr as STRING) as profit_ctr,
            CAST(paobjnr as STRING) as prof_seg,
            CAST(wwprd as STRING) as nomor_produk_kai,
            CAST(bezek as STRING) as name,
            CAST(pspnr as STRING) as wbs_element,
            CAST(posid as STRING) as wbs_element_2,
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
    'dashboard_investasi-ztb_dashb_mm0002',
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
            "banfn"::TEXT as "banfn",
            "bnfpo"::TEXT as "bnfpo",
            "zebkn"::TEXT as "zebkn",
            "zyear"::TEXT as "zyear",
            "loekz"::TEXT as "loekz",
            "gsber"::TEXT as "gsber",
            "sakto"::TEXT as "sakto",
            "kostl"::TEXT as "kostl",
            "aufnr"::TEXT as "aufnr",
            "anln1"::TEXT as "anln1",
            "netwr"::NUMERIC as "netwr",
            "fistl"::TEXT as "fistl",
            "fipos"::TEXT as "fipos",
            "prctr"::TEXT as "prctr",
            "paobjnr"::TEXT as "paobjnr",
            "wwprd"::TEXT as "wwprd",
            "bezek"::TEXT as "bezek",
            "pspnr"::TEXT as "pspnr",
            "posid"::TEXT as "posid",
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
