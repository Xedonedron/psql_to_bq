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
TABLE = 'ztb_dashb_mm0001'
ALTER_TABLE = "data_monitoring_pengadaan"
GCS_BUCKET = 'kai_sap'
FILE_NAME = f'{TABLE}.parquet_part{{}}'

def push_current_timestamp():
    jakarta_tz = pytz.timezone('Asia/Jakarta')
    current_ts = datetime.now(jakarta_tz).strftime('%Y-%m-%d %H:%M:%S')
    return current_ts  # Push to XCom automatically


def build_query_and_run(**kwargs):
    ti = kwargs['ti']
    current_ts = ti.xcom_pull(task_ids='push_ts_task')

    query = f"""
        INSERT INTO {PROJECT_ID}.{SCHEMA}.{ALTER_TABLE} (client, purchreq, item, purchdoc, item_2, matdoc, matyr, getdatayear, entrysh, a, i, material, shorttext, plnt, qtyrequested, un, totalvaluponrelease, totalvaluponrelease_2, crcy, chngdon, reqdate, requisitionrequestenddate, number, d, fundsctr, commitmentitm, rfqnumber, rfqitem, rfqdate, rfqenddate, quotationdate, quotationenddate, nomorkontrak, vpstart, delivdate, vendor, name1, exchrate, fix, totalvaluponrelease_3, totalvaluponrelease_4, crcy_2, number_2, d_2, changedon, podate, purchaseorderenddate, createdon, tx, docdate, pstngdate, rel, relstat, pgr, matlgroup, sloc, last_insert_to_bigquery)
        SELECT
            CAST(mandt as STRING) AS client,
            CAST(banfn as STRING) AS purchreq,
            CAST(bnfpo as STRING) AS item,
            CAST(ebeln as STRING) AS purchdoc,
            CAST(ebelp as STRING) AS item_2,
            CAST(mblnr as STRING) AS matdoc,
            CAST(mjahr as STRING) AS matyr,
            CAST(zyear as STRING) AS getdatayear,
            CAST(lblni as STRING) AS entrysh,
            CAST(knttp as STRING) AS a,
            CAST(pstyp as STRING) AS i,
            CAST(matnr as STRING) AS material,
            CAST(txz01 as STRING) AS shorttext,
            CAST(werks as STRING) AS plnt,
            CAST(menge as numeric) AS qtyrequested,
            CAST(meins as STRING) AS un,
            CAST(preis as numeric) AS totalvaluponrelease,
            CAST(rlwrt as numeric) AS totalvaluponrelease_2,
            CAST(waerspr as STRING) AS crcy,
            CAST(aedatpr as STRING) AS chngdon,
            CAST(badat as STRING) AS reqdate,
            CAST(zendpr as STRING) AS requisitionrequestenddate,
            CAST(packnopr as STRING) AS number,
            CAST(loekzpr as STRING) AS d,
            CAST(fistl as STRING) AS fundsctr,
            CAST(fipos as STRING) AS commitmentitm,
            CAST(zrfqnum as STRING) AS rfqnumber,
            CAST(zrfqitem as STRING) AS rfqitem,
            CAST(zrfqdate as STRING) AS rfqdate,
            CAST(zendrfq as STRING) AS rfqenddate,
            CAST(zquotdate as STRING) AS quotationdate,
            CAST(zendquot as STRING) AS quotationenddate,
            CAST(zkontrak as STRING) AS nomorkontrak,
            CAST(kdatb as STRING) AS vpstart,
            CAST(eindt as STRING) AS delivdate,
            CAST(lifnr as STRING) AS vendor,
            CAST(name1 as STRING) AS name1,
            CAST(wkurs as numeric) AS exchrate,
            CAST(kufix as STRING) AS fix,
            CAST(netpr as numeric) AS totalvaluponrelease_3,
            CAST(netwr as numeric) AS totalvaluponrelease_4,
            CAST(waerspo as STRING) AS crcy_2,
            CAST(packnopo as STRING) AS number_2,
            CAST(loekzpo as STRING) AS d_2,
            CAST(aedatpo as STRING) AS changedon,
            CAST(bedat as STRING) AS podate,
            CAST(zendpo as STRING) AS purchaseorderenddate,
            CAST(erdat as STRING) AS createdon,
            CAST(mwskz as STRING) AS tx,
            CAST(bldat as STRING) AS docdate,
            CAST(budat as STRING) AS pstngdate,
            CAST(frgkz as STRING) AS rel,
            CAST(frgzu as STRING) AS relstat,
            CAST(ekgrp as STRING) AS pgr,
            CAST(matkl as STRING) AS matlgroup,
            CAST(lgort as STRING) AS sloc,
            CAST(netwr_ktl as NUMERIC) AS ,
            CAST(last_update as DATETIME) AS last_update,
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
    'dashboard_investasi-ztb_dashb_mm0001',
    description="Doing incremental load from PostgreSQL to GCS",
    start_date=pendulum.datetime(2024, 9, 30, tz="Asia/Jakarta"),
    schedule_interval='0 2 * * *',
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
            "ebeln"::TEXT as "ebeln",
            "ebelp"::TEXT as "ebelp",
            "mblnr"::TEXT as "mblnr",
            "mjahr"::TEXT as "mjahr",
            "zyear"::TEXT as "zyear",
            "lblni"::TEXT as "lblni",
            "knttp"::TEXT as "knttp",
            "pstyp"::TEXT as "pstyp",
            "matnr"::TEXT as "matnr",
            "txz01"::TEXT as "txz01",
            "werks"::TEXT as "werks",
            "meins"::TEXT as "meins",
            "waerspr"::TEXT as "waerspr",
            "aedatpr"::TEXT as "aedatpr",
            "badat"::TEXT as "badat",
            "zendpr"::TEXT as "zendpr",
            "packnopr"::TEXT as "packnopr",
            "loekzpr"::TEXT as "loekzpr",
            "fistl"::TEXT as "fistl",
            "fipos"::TEXT as "fipos",
            "zrfqnum"::TEXT as "zrfqnum",
            "zrfqitem"::TEXT as "zrfqitem",
            "zrfqdate"::TEXT as "zrfqdate",
            "zendrfq"::TEXT as "zendrfq",
            "zquotdate"::TEXT as "zquotdate",
            "zendquot"::TEXT as "zendquot",
            "zkontrak"::TEXT as "zkontrak",
            "kdatb"::TEXT as "kdatb",
            "lifnr"::TEXT as "lifnr",
            "name1"::TEXT as "name1",
            "kufix"::TEXT as "kufix",
            "netwr"::NUMERIC as "netwr",
            "waerspo"::TEXT as "waerspo",
            "packnopo"::TEXT as "packnopo",
            "loekzpo"::TEXT as "loekzpo",
            "aedatpo"::TEXT as "aedatpo",
            "bedat"::TEXT as "bedat",
            "zendpo"::TEXT as "zendpo",
            "erdat"::TEXT as "erdat",
            "mwskz"::TEXT as "mwskz",
            "bldat"::TEXT as "bldat",
            "budat"::TEXT as "budat",
            "eindt"::TEXT as "eindt",
            "menge"::NUMERIC as "menge",
            "preis"::NUMERIC as "preis",
            "rlwrt"::NUMERIC as "rlwrt",
            "wkurs"::NUMERIC as "wkurs",
            "netpr"::NUMERIC as "netpr",
            "netwr_ktl"::NUMERIC as "netwr_ktl",
            "last_update"::TEXT as "last_update",
            "frgkz"::TEXT as "frgkz",
            "frgzu"::TEXT as "frgzu",
            "ekgrp"::TEXT as "ekgrp",
            "matkl"::TEXT as "matkl",
            "lgort"::TEXT as "lgort"
        FROM {SCHEMA}."{TABLE}";
        """,
        bucket=GCS_BUCKET,
        filename=f'{SCHEMA}/{TABLE}/{FILE_NAME}',  # Nama file yang disimpan di GCS
        export_format='parquet',
        gcp_conn_id='kai_genai_prod',
        approx_max_file_size_bytes=20 * 1024 * 1024, # split 20 MB
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
