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
TABLE = 'project_timeline'
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
            CAST(posid_1 as STRING) as posid_1,
            CAST(posid_2 as STRING) as posid_2,
            CAST(posid_3 as STRING) as posid_3,
            CAST(posid_4 as STRING) as posid_4,
            CAST(posid_5 as STRING) as posid_5,
            CAST(post1_1 as STRING) as post1_1,
            CAST(post1_2 as STRING) as post1_2,
            CAST(post1_3 as STRING) as post1_3,
            CAST(post1_4 as STRING) as post1_4,
            CAST(post1_5 as STRING) as post1_5,
            CAST(pgsbr as STRING) as pgsbr,
            CAST(pstrt as STRING) as pstrt,
            CAST(pende as STRING) as pende,
            CAST(istrt as STRING) as istrt,
            CAST(iende as STRING) as iende,
            CAST(meinh as STRING) as meinh,
            CAST(objnr as STRING) as objnr,
            CAST(pspnr as STRING) as pspnr,
            CAST(zprogplan as NUMERIC) as zprogplan,
            CAST(zprogact as NUMERIC) as zprogact,
            CAST(usr00 as STRING) as usr00,
            CAST(usr01 as STRING) as usr01,
            CAST(usr02 as STRING) as usr02,
            CAST(usr03 as STRING) as usr03,
            CAST(usr04 as NUMERIC) as usr04,
            CAST(use04 as STRING) as use04,
            CAST(usr05 as NUMERIC) as usr05,
            CAST(use05 as STRING) as use05,
            CAST(usr06 as NUMERIC) as usr06,
            CAST(use06 as STRING) as use06,
            CAST(usr07 as NUMERIC) as usr07,
            CAST(use07 as STRING) as use07,
            CAST(usr08 as STRING) as usr08,
            CAST(usr09 as STRING) as usr09,
            CAST(usr10 as STRING) as usr10,
            CAST(usr11 as STRING) as usr11,
            CAST(last_update as STRING) as last_update,
            CAST(vernr as STRING) as vernr,
            CAST(verna as STRING) as verna,
            CAST(astnr as STRING) as astnr,
            CAST(astna as STRING) as astna,
            CAST(znm_usr00 as STRING) as znm_usr00,
            CAST(znm_usr02 as STRING) as znm_usr02,
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
    'dashboard_investasi-project_timeline',
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
            "posid_1"::TEXT as "posid_1",
            "posid_2"::TEXT as "posid_2",
            "posid_3"::TEXT as "posid_3",
            "posid_4"::TEXT as "posid_4",
            "posid_5"::TEXT as "posid_5",
            "post1_1"::TEXT as "post1_1",
            "post1_2"::TEXT as "post1_2",
            "post1_3"::TEXT as "post1_3",
            "post1_4"::TEXT as "post1_4",
            "post1_5"::TEXT as "post1_5",
            "pgsbr"::TEXT as "pgsbr",
            "pstrt"::TEXT as "pstrt",
            "pende"::TEXT as "pende",
            "istrt"::TEXT as "istrt",
            "iende"::TEXT as "iende",
            "meinh"::TEXT as "meinh",
            "objnr"::TEXT as "objnr",
            "pspnr"::TEXT as "pspnr",
            "zprogplan"::NUMERIC as "zprogplan",
            "zprogact"::NUMERIC as "zprogact",
            "usr00"::TEXT as "usr00",
            "usr01"::TEXT as "usr01",
            "usr02"::TEXT as "usr02",
            "usr03"::TEXT as "usr03",
            "usr04"::NUMERIC as "usr04",
            "use04"::TEXT as "use04",
            "usr05"::NUMERIC as "usr05",
            "use05"::TEXT as "use05",
            "usr06"::NUMERIC as "usr06",
            "use06"::TEXT as "use06",
            "usr07"::NUMERIC as "usr07",
            "use07"::TEXT as "use07",
            "usr08"::TEXT as "usr08",
            "usr09"::TEXT as "usr09",
            "usr10"::TEXT as "usr10",
            "usr11"::TEXT as "usr11",
            "last_update"::TEXT as "last_update",
            "vernr"::TEXT as "vernr",
            "verna"::TEXT as "verna",
            "astnr"::TEXT as "astnr",
            "astna"::TEXT as "astna",
            "znm_usr00"::TEXT as "znm_usr00",
            "znm_usr02"::TEXT as "znm_usr02"
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
