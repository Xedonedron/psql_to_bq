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
TABLE = 'Comitment_Doc_Funds_Management'
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
            CAST(MANDT as STRING) as MANDT,
            CAST(REFBN as STRING) as REFBN,
            CAST(REFBT as STRING) as REFBT,
            CAST(RFORG as STRING) as RFORG,
            CAST(RFPOS as STRING) as RFPOS,
            CAST(RFKNT as STRING) as RFKNT,
            CAST(RFETE as STRING) as RFETE,
            CAST(RCOND as STRING) as RCOND,
            CAST(RFTYP as STRING) as RFTYP,
            CAST(RFSYS as STRING) as RFSYS,
            CAST(BTART as STRING) as BTART,
            CAST(RLDNR as STRING) as RLDNR,
            CAST(GJAHR as STRING) as GJAHR,
            CAST(STUNR as STRING) as STUNR,
            CAST(ZHLDT as STRING) as ZHLDT,
            CAST(GNJHR as STRING) as GNJHR,
            CAST(PERIO as STRING) as PERIO,
            CAST(CFSTAT as STRING) as CFSTAT,
            CAST(CFSTATSV as STRING) as CFSTATSV,
            CAST(CFCNT as STRING) as CFCNT,
            CAST(OBJNRZ as STRING) as OBJNRZ,
            CAST(TRBTR as NUMERIC) as TRBTR,
            CAST(FKBTR as NUMERIC) as FKBTR,
            CAST(FISTL as STRING) as FISTL,
            CAST(FONDS as STRING) as FONDS,
            CAST(FIPEX as STRING) as FIPEX,
            CAST(FAREA as STRING) as FAREA,
            CAST(MEASURE as STRING) as MEASURE,
            CAST(GRANT_NBR as STRING) as GRANT_NBR,
            CAST(BUS_AREA as STRING) as BUS_AREA,
            CAST(PRCTR as STRING) as PRCTR,
            CAST(WRTTP as STRING) as WRTTP,
            CAST(VRGNG as STRING) as VRGNG,
            CAST(BUKRS as STRING) as BUKRS,
            CAST(STATS as STRING) as STATS,
            CAST(TWAER as STRING) as TWAER,
            CAST(CFLEV as STRING) as CFLEV,
            CAST(SGTXT as STRING) as SGTXT,
            CAST(TRANR as STRING) as TRANR,
            CAST(CTRNR as STRING) as CTRNR,
            CAST(USERDIM as STRING) as USERDIM,
            CAST(FMVOR as STRING) as FMVOR,
            CAST(BUDGET_PD as STRING) as BUDGET_PD,
            CAST(HKONT as STRING) as HKONT,
            CAST(ERLKZ as STRING) as ERLKZ,
            CAST(LOEKZ as STRING) as LOEKZ,
            CAST(FIKRS as STRING) as FIKRS,
            CAST(BUDAT as STRING) as BUDAT,
            CAST(LIFNR as STRING) as LIFNR,
            CAST(BL_DOC_TYPE as STRING) as BL_DOC_TYPE,
            CAST(DP_WITH_PO as STRING) as DP_WITH_PO,
            CAST(VREFBT as STRING) as VREFBT,
            CAST(VREFBN as STRING) as VREFBN,
            CAST(VRFORG as STRING) as VRFORG,
            CAST(VRFPOS as STRING) as VRFPOS,
            CAST(VRFKNT as STRING) as VRFKNT,
            CAST(VRFTYP as STRING) as VRFTYP,
            CAST(VRFSYS as STRING) as VRFSYS,
            CAST(ITCNACT as STRING) as ITCNACT,
            CAST(CPUDT as STRING) as CPUDT,
            CAST(CPUTM as STRING) as CPUTM,
            CAST(USNAM as STRING) as USNAM,
            CAST(TCODE as STRING) as TCODE,
            CAST(BLDOCDATE as STRING) as BLDOCDATE,
            CAST(QUANT_OPEN as NUMERIC) as QUANT_OPEN,
            CAST(QUANT_INV as NUMERIC) as QUANT_INV,
            CAST(MEINS as STRING) as MEINS,
            CAST(KUNNR as STRING) as KUNNR,
            CAST(ZZTXJNS as STRING) as ZZTXJNS,
            CAST(ZZTRANSDOC as STRING) as ZZTRANSDOC,
            CAST(ZZJNSDOC as STRING) as ZZJNSDOC,
            CAST(ZZTXTRANSID as STRING) as ZZTXTRANSID,
            CAST(ZZTXSTS as STRING) as ZZTXSTS,
            CAST(ZZTXINVO as STRING) as ZZTXINVO,
            CAST(ZZTXDATE as STRING) as ZZTXDATE,
            CAST(ZZREPDOC as STRING) as ZZREPDOC,
            CAST(ZZPPN as STRING) as ZZPPN,
            CAST(lastupdate as STRING) as lastupdate,
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
    'dashboard_kai-Comitment_Doc_Funds_Management',
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
            "MANDT"::TEXT as "MANDT",
            "REFBN"::TEXT as "REFBN",
            "REFBT"::TEXT as "REFBT",
            "RFORG"::TEXT as "RFORG",
            "RFPOS"::TEXT as "RFPOS",
            "RFKNT"::TEXT as "RFKNT",
            "RFETE"::TEXT as "RFETE",
            "RCOND"::TEXT as "RCOND",
            "RFTYP"::TEXT as "RFTYP",
            "RFSYS"::TEXT as "RFSYS",
            "BTART"::TEXT as "BTART",
            "RLDNR"::TEXT as "RLDNR",
            "GJAHR"::TEXT as "GJAHR",
            "STUNR"::TEXT as "STUNR",
            "ZHLDT"::TEXT as "ZHLDT",
            "GNJHR"::TEXT as "GNJHR",
            "PERIO"::TEXT as "PERIO",
            "CFSTAT"::TEXT as "CFSTAT",
            "CFSTATSV"::TEXT as "CFSTATSV",
            "CFCNT"::TEXT as "CFCNT",
            "OBJNRZ"::TEXT as "OBJNRZ",
            "TRBTR"::NUMERIC as "TRBTR",
            "FKBTR"::NUMERIC as "FKBTR",
            "FISTL"::TEXT as "FISTL",
            "FONDS"::TEXT as "FONDS",
            "FIPEX"::TEXT as "FIPEX",
            "FAREA"::TEXT as "FAREA",
            "MEASURE"::TEXT as "MEASURE",
            "GRANT_NBR"::TEXT as "GRANT_NBR",
            "BUS_AREA"::TEXT as "BUS_AREA",
            "PRCTR"::TEXT as "PRCTR",
            "WRTTP"::TEXT as "WRTTP",
            "VRGNG"::TEXT as "VRGNG",
            "BUKRS"::TEXT as "BUKRS",
            "STATS"::TEXT as "STATS",
            "TWAER"::TEXT as "TWAER",
            "CFLEV"::TEXT as "CFLEV",
            "SGTXT"::TEXT as "SGTXT",
            "TRANR"::TEXT as "TRANR",
            "CTRNR"::TEXT as "CTRNR",
            "USERDIM"::TEXT as "USERDIM",
            "FMVOR"::TEXT as "FMVOR",
            "BUDGET_PD"::TEXT as "BUDGET_PD",
            "HKONT"::TEXT as "HKONT",
            "ERLKZ"::TEXT as "ERLKZ",
            "LOEKZ"::TEXT as "LOEKZ",
            "FIKRS"::TEXT as "FIKRS",
            "BUDAT"::TEXT as "BUDAT",
            "LIFNR"::TEXT as "LIFNR",
            "BL_DOC_TYPE"::TEXT as "BL_DOC_TYPE",
            "DP_WITH_PO"::TEXT as "DP_WITH_PO",
            "VREFBT"::TEXT as "VREFBT",
            "VREFBN"::TEXT as "VREFBN",
            "VRFORG"::TEXT as "VRFORG",
            "VRFPOS"::TEXT as "VRFPOS",
            "VRFKNT"::TEXT as "VRFKNT",
            "VRFTYP"::TEXT as "VRFTYP",
            "VRFSYS"::TEXT as "VRFSYS",
            "ITCNACT"::TEXT as "ITCNACT",
            "CPUDT"::TEXT as "CPUDT",
            "CPUTM"::TEXT as "CPUTM",
            "USNAM"::TEXT as "USNAM",
            "TCODE"::TEXT as "TCODE",
            "BLDOCDATE"::TEXT as "BLDOCDATE",
            "QUANT_OPEN"::NUMERIC as "QUANT_OPEN",
            "QUANT_INV"::NUMERIC as "QUANT_INV",
            "MEINS"::TEXT as "MEINS",
            "KUNNR"::TEXT as "KUNNR",
            "ZZTXJNS"::TEXT as "ZZTXJNS",
            "ZZTRANSDOC"::TEXT as "ZZTRANSDOC",
            "ZZJNSDOC"::TEXT as "ZZJNSDOC",
            "ZZTXTRANSID"::TEXT as "ZZTXTRANSID",
            "ZZTXSTS"::TEXT as "ZZTXSTS",
            "ZZTXINVO"::TEXT as "ZZTXINVO",
            "ZZTXDATE"::TEXT as "ZZTXDATE",
            "ZZREPDOC"::TEXT as "ZZREPDOC",
            "ZZPPN"::TEXT as "ZZPPN",
            "lastupdate"::TEXT as "lastupdate"
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
