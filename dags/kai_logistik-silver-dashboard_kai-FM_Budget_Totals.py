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
SCHEMA = 'dashboard_kai'
POSTGRES_CONNECTION_ID = 'kai_postgres'
TABLE = 'FM_Budget_Totals'
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
            CAST(RCLNT as STRING) as RCLNT,
            CAST(RLDNR as STRING) as RLDNR,
            CAST(RRCTY as STRING) as RRCTY,
            CAST(RVERS as STRING) as RVERS,
            CAST(RYEAR as STRING) as RYEAR,
            CAST(ROBJNR as STRING) as ROBJNR,
            CAST(COBJNR as STRING) as COBJNR,
            CAST(SOBJNR as STRING) as SOBJNR,
            CAST(RTCUR as STRING) as RTCUR,
            CAST(DRCRK as STRING) as DRCRK,
            CAST(RPMAX as STRING) as RPMAX,
            CAST(RFIKRS as STRING) as RFIKRS,
            CAST(RFUND as STRING) as RFUND,
            CAST(RFUNDSCTR as STRING) as RFUNDSCTR,
            CAST(RCMMTITEM as STRING) as RCMMTITEM,
            CAST(RFUNCAREA as STRING) as RFUNCAREA,
            CAST(RUSERDIM as STRING) as RUSERDIM,
            CAST(RGRANT_NBR as STRING) as RGRANT_NBR,
            CAST(RMEASURE as STRING) as RMEASURE,
            CAST(CEFFYEAR_9 as STRING) as CEFFYEAR_9,
            CAST(VALTYPE_9 as STRING) as VALTYPE_9,
            CAST(WFSTATE_9 as STRING) as WFSTATE_9,
            CAST(PROCESS_9 as STRING) as PROCESS_9,
            CAST(BUDTYPE_9 as STRING) as BUDTYPE_9,
            CAST(LOGSYS as STRING) as LOGSYS,
            CAST(TSLVT as NUMERIC) as TSLVT,
            CAST(TSL01 as NUMERIC) as TSL01,
            CAST(TSL02 as NUMERIC) as TSL02,
            CAST(TSL03 as NUMERIC) as TSL03,
            CAST(TSL04 as NUMERIC) as TSL04,
            CAST(TSL05 as NUMERIC) as TSL05,
            CAST(TSL06 as NUMERIC) as TSL06,
            CAST(TSL07 as NUMERIC) as TSL07,
            CAST(TSL08 as NUMERIC) as TSL08,
            CAST(TSL09 as NUMERIC) as TSL09,
            CAST(TSL10 as NUMERIC) as TSL10,
            CAST(TSL11 as NUMERIC) as TSL11,
            CAST(TSL12 as NUMERIC) as TSL12,
            CAST(TSL13 as NUMERIC) as TSL13,
            CAST(TSL14 as NUMERIC) as TSL14,
            CAST(TSL15 as NUMERIC) as TSL15,
            CAST(TSL16 as NUMERIC) as TSL16,
            CAST(HSLVT as NUMERIC) as HSLVT,
            CAST(HSL01 as NUMERIC) as HSL01,
            CAST(HSL02 as NUMERIC) as HSL02,
            CAST(HSL03 as NUMERIC) as HSL03,
            CAST(HSL04 as NUMERIC) as HSL04,
            CAST(HSL05 as NUMERIC) as HSL05,
            CAST(HSL06 as NUMERIC) as HSL06,
            CAST(HSL07 as NUMERIC) as HSL07,
            CAST(HSL08 as NUMERIC) as HSL08,
            CAST(HSL09 as NUMERIC) as HSL09,
            CAST(HSL10 as NUMERIC) as HSL10,
            CAST(HSL11 as NUMERIC) as HSL11,
            CAST(HSL12 as NUMERIC) as HSL12,
            CAST(HSL13 as NUMERIC) as HSL13,
            CAST(HSL14 as NUMERIC) as HSL14,
            CAST(HSL15 as NUMERIC) as HSL15,
            CAST(HSL16 as NUMERIC) as HSL16,
            CAST(KSLVT as NUMERIC) as KSLVT,
            CAST(KSL01 as NUMERIC) as KSL01,
            CAST(KSL02 as NUMERIC) as KSL02,
            CAST(KSL03 as NUMERIC) as KSL03,
            CAST(KSL04 as NUMERIC) as KSL04,
            CAST(KSL05 as NUMERIC) as KSL05,
            CAST(KSL06 as NUMERIC) as KSL06,
            CAST(KSL07 as NUMERIC) as KSL07,
            CAST(KSL08 as NUMERIC) as KSL08,
            CAST(KSL09 as NUMERIC) as KSL09,
            CAST(KSL10 as NUMERIC) as KSL10,
            CAST(KSL11 as NUMERIC) as KSL11,
            CAST(KSL12 as NUMERIC) as KSL12,
            CAST(KSL13 as NUMERIC) as KSL13,
            CAST(KSL14 as NUMERIC) as KSL14,
            CAST(KSL15 as NUMERIC) as KSL15,
            CAST(KSL16 as NUMERIC) as KSL16,
            CAST(CSPRED as STRING) as CSPRED,
            CAST(CTEM_CATEGORY_9 as STRING) as CTEM_CATEGORY_9,
            CAST(lastupdate as DATETIME) as lastupdate,
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
    'dashboard_kai-FM_Budget_Totals',
    description="Doing incremental load from PostgreSQL to GCS",
    start_date=pendulum.datetime(2024, 9, 30, tz="Asia/Jakarta"),
    schedule_interval='* 1 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['Gen-AI', 'dashboard_kai', 'refined'],
) as dag:

    dump_from_postgres_to_gcs = PostgresToGCSOperator(
        task_id='dump_from_postgres_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql=f"""
        SELECT
            "RCLNT"::TEXT as "RCLNT",
            "RLDNR"::TEXT as "RLDNR",
            "RRCTY"::TEXT as "RRCTY",
            "RVERS"::TEXT as "RVERS",
            "RYEAR"::TEXT as "RYEAR",
            "ROBJNR"::TEXT as "ROBJNR",
            "COBJNR"::TEXT as "COBJNR",
            "SOBJNR"::TEXT as "SOBJNR",
            "RTCUR"::TEXT as "RTCUR",
            "DRCRK"::TEXT as "DRCRK",
            "RPMAX"::TEXT as "RPMAX",
            "RFIKRS"::TEXT as "RFIKRS",
            "RFUND"::TEXT as "RFUND",
            "RFUNDSCTR"::TEXT as "RFUNDSCTR",
            "RCMMTITEM"::TEXT as "RCMMTITEM",
            "RFUNCAREA"::TEXT as "RFUNCAREA",
            "RUSERDIM"::TEXT as "RUSERDIM",
            "RGRANT_NBR"::TEXT as "RGRANT_NBR",
            "RMEASURE"::TEXT as "RMEASURE",
            "CEFFYEAR_9"::TEXT as "CEFFYEAR_9",
            "VALTYPE_9"::TEXT as "VALTYPE_9",
            "WFSTATE_9"::TEXT as "WFSTATE_9",
            "PROCESS_9"::TEXT as "PROCESS_9",
            "BUDTYPE_9"::TEXT as "BUDTYPE_9",
            "LOGSYS"::TEXT as "LOGSYS",
            "TSLVT"::NUMERIC as "TSLVT",
            "TSL01"::NUMERIC as "TSL01",
            "TSL02"::NUMERIC as "TSL02",
            "TSL03"::NUMERIC as "TSL03",
            "TSL04"::NUMERIC as "TSL04",
            "TSL05"::NUMERIC as "TSL05",
            "TSL06"::NUMERIC as "TSL06",
            "TSL07"::NUMERIC as "TSL07",
            "TSL08"::NUMERIC as "TSL08",
            "TSL09"::NUMERIC as "TSL09",
            "TSL10"::NUMERIC as "TSL10",
            "TSL11"::NUMERIC as "TSL11",
            "TSL12"::NUMERIC as "TSL12",
            "TSL13"::NUMERIC as "TSL13",
            "TSL14"::NUMERIC as "TSL14",
            "TSL15"::NUMERIC as "TSL15",
            "TSL16"::NUMERIC as "TSL16",
            "HSLVT"::NUMERIC as "HSLVT",
            "HSL01"::NUMERIC as "HSL01",
            "HSL02"::NUMERIC as "HSL02",
            "HSL03"::NUMERIC as "HSL03",
            "HSL04"::NUMERIC as "HSL04",
            "HSL05"::NUMERIC as "HSL05",
            "HSL06"::NUMERIC as "HSL06",
            "HSL07"::NUMERIC as "HSL07",
            "HSL08"::NUMERIC as "HSL08",
            "HSL09"::NUMERIC as "HSL09",
            "HSL10"::NUMERIC as "HSL10",
            "HSL11"::NUMERIC as "HSL11",
            "HSL12"::NUMERIC as "HSL12",
            "HSL13"::NUMERIC as "HSL13",
            "HSL14"::NUMERIC as "HSL14",
            "HSL15"::NUMERIC as "HSL15",
            "HSL16"::NUMERIC as "HSL16",
            "KSLVT"::NUMERIC as "KSLVT",
            "KSL01"::NUMERIC as "KSL01",
            "KSL02"::NUMERIC as "KSL02",
            "KSL03"::NUMERIC as "KSL03",
            "KSL04"::NUMERIC as "KSL04",
            "KSL05"::NUMERIC as "KSL05",
            "KSL06"::NUMERIC as "KSL06",
            "KSL07"::NUMERIC as "KSL07",
            "KSL08"::NUMERIC as "KSL08",
            "KSL09"::NUMERIC as "KSL09",
            "KSL10"::NUMERIC as "KSL10",
            "KSL11"::NUMERIC as "KSL11",
            "KSL12"::NUMERIC as "KSL12",
            "KSL13"::NUMERIC as "KSL13",
            "KSL14"::NUMERIC as "KSL14",
            "KSL15"::NUMERIC as "KSL15",
            "KSL16"::NUMERIC as "KSL16",
            "CSPRED"::TEXT as "CSPRED",
            "CTEM_CATEGORY_9"::TEXT as "CTEM_CATEGORY_9",
            "lastupdate"::TEXT as "lastupdate"
        FROM {SCHEMA}."{TABLE}";
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
