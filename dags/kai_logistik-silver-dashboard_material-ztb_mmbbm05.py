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
TABLE = 'ztb_mmbbm05'
GCS_BUCKET = 'kai_sap'
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
            CAST(MANDT as STRING) as MANDT,
            CAST(MBLNR as STRING) as MBLNR,
            CAST(MJAHR as STRING) as MJAHR,
            CAST(MATNR as STRING) as MATNR,
            CAST(NOKA as STRING) as NOKA,
            CAST(BKTXT as STRING) as BKTXT,
            CAST(XBLNR as STRING) as XBLNR,
            CAST(BUDAT as STRING) as BUDAT,
            CAST(BLDAT as STRING) as BLDAT,
            CAST(CPUDT as STRING) as CPUDT,
            CAST(CPUTM as STRING) as CPUTM,
            CAST(WERKS as STRING) as WERKS,
            CAST(NAME1 as STRING) as NAME1,
            CAST(BWART as STRING) as BWART,
            CAST(ERFMG as NUMERIC) as ERFMG,
            CAST(ERFME as STRING) as ERFME,
            CAST(DMBTR as NUMERIC) as DMBTR,
            CAST(WEMPF as STRING) as WEMPF,
            CAST(SGTXT as STRING) as SGTXT,
            CAST(SHKZG as STRING) as SHKZG,
            CAST(SOBKZ as STRING) as SOBKZ,
            CAST(SMBLN as STRING) as SMBLN,
            CAST(RSNUM as STRING) as RSNUM,
            CAST(AUFNR as STRING) as AUFNR,
            CAST(EQUNR as STRING) as EQUNR,
            CAST(QMNUM as STRING) as QMNUM,
            CAST(KTEXT as STRING) as KTEXT,
            CAST(AUART as STRING) as AUART,
            CAST(PWERKS as STRING) as PWERKS,
            CAST(VAPLZ as STRING) as VAPLZ,
            CAST(KTEXT_UP as STRING) as KTEXT_UP,
            CAST(SOWRK as STRING) as SOWRK,
            CAST(EQFNR as STRING) as EQFNR,
            CAST(EQKTX as STRING) as EQKTX,
            CAST(ARBPL as STRING) as ARBPL,
            CAST(AUSVN as STRING) as AUSVN,
            CAST(AUSBS as STRING) as AUSBS,
            CAST(AUZTV as STRING) as AUZTV,
            CAST(AUZTB as STRING) as AUZTB,
            CAST(TDATE as NUMERIC) as TDATE,
            CAST(OBJTY as STRING) as OBJTY,
            CAST(OBJID as STRING) as OBJID,
            CAST(BLN as STRING) as BLN,
            CAST(NMKA as STRING) as NMKA,
            CAST(THN_GAP as STRING) as THN_GAP,
            CAST(QTY_SPLIT as NUMERIC) as QTY_SPLIT,
            CAST(MAKTX as STRING) as MAKTX,
            CAST(BTEXT as STRING) as BTEXT,
            CAST(ABLAD as STRING) as ABLAD,
            CAST(SAKTO as STRING) as SAKTO,
            CAST(ZSISA as NUMERIC) as ZSISA,
            CAST(ID_KOM as STRING) as ID_KOM,
            CAST(KOMODITI as STRING) as KOMODITI,
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
    'dashboard_material-ztb_mmbbm05',
    description="Doing incremental load from PostgreSQL to GCS",
    start_date=pendulum.datetime(2024, 9, 30, tz="Asia/Jakarta"),
    schedule_interval='0 3 * * *',
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
            "MBLNR"::TEXT as "MBLNR",
            "MJAHR"::TEXT as "MJAHR",
            "MATNR"::TEXT as "MATNR",
            "NOKA"::TEXT as "NOKA",
            "BKTXT"::TEXT as "BKTXT",
            "XBLNR"::TEXT as "XBLNR",
            "BUDAT"::TEXT as "BUDAT",
            "BLDAT"::TEXT as "BLDAT",
            "CPUDT"::TEXT as "CPUDT",
            "CPUTM"::TEXT as "CPUTM",
            "WERKS"::TEXT as "WERKS",
            "NAME1"::TEXT as "NAME1",
            "BWART"::TEXT as "BWART",
            "ERFMG"::NUMERIC as "ERFMG",
            "ERFME"::TEXT as "ERFME",
            "DMBTR"::NUMERIC as "DMBTR",
            "WEMPF"::TEXT as "WEMPF",
            "SGTXT"::TEXT as "SGTXT",
            "SHKZG"::TEXT as "SHKZG",
            "SOBKZ"::TEXT as "SOBKZ",
            "SMBLN"::TEXT as "SMBLN",
            "RSNUM"::TEXT as "RSNUM",
            "AUFNR"::TEXT as "AUFNR",
            "EQUNR"::TEXT as "EQUNR",
            "QMNUM"::TEXT as "QMNUM",
            "KTEXT"::TEXT as "KTEXT",
            "AUART"::TEXT as "AUART",
            "PWERKS"::TEXT as "PWERKS",
            "VAPLZ"::TEXT as "VAPLZ",
            "KTEXT_UP"::TEXT as "KTEXT_UP",
            "SOWRK"::TEXT as "SOWRK",
            "EQFNR"::TEXT as "EQFNR",
            "EQKTX"::TEXT as "EQKTX",
            "ARBPL"::TEXT as "ARBPL",
            "AUSVN"::TEXT as "AUSVN",
            "AUSBS"::TEXT as "AUSBS",
            "AUZTV"::TEXT as "AUZTV",
            "AUZTB"::TEXT as "AUZTB",
            "TDATE"::NUMERIC as "TDATE",
            "OBJTY"::TEXT as "OBJTY",
            "OBJID"::TEXT as "OBJID",
            "BLN"::TEXT as "BLN",
            "NMKA"::TEXT as "NMKA",
            "THN_GAP"::TEXT as "THN_GAP",
            "QTY_SPLIT"::NUMERIC as "QTY_SPLIT",
            "MAKTX"::TEXT as "MAKTX",
            "BTEXT"::TEXT as "BTEXT",
            "ABLAD"::TEXT as "ABLAD",
            "SAKTO"::TEXT as "SAKTO",
            "ZSISA"::NUMERIC as "ZSISA",
            "ID_KOM"::TEXT as "ID_KOM",
            "KOMODITI"::TEXT as "KOMODITI"
        FROM {SCHEMA}.{TABLE};
        """,
        bucket=GCS_BUCKET,
        filename=f'{SCHEMA}/{TABLE}/{FILE_NAME}',  # Nama file yang disimpan di GCS
        export_format='parquet',
        gcp_conn_id='kai_genai_prod',
        approx_max_file_size_bytes=20 * 1024 * 1024, # split 50 MB
        # parquet_row_group_size=1000000,
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
