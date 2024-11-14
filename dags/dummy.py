import pendulum

from airflow import models
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import pytz
   
with models.DAG(
    'dummy-DAG',
    description="Dummy for checking",
    start_date=pendulum.datetime(2024, 9, 30, tz="Asia/Jakarta"),
    schedule_interval='* 1 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['Gen-AI', 'TESTING'],
) as dag:
    
    dummy_task = DummyOperator(
        task_id="dummy_task",
    )

    dummy_task