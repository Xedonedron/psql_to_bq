import pendulum

from airflow import models
from airflow.operators.bash import BashOperator
from datetime import datetime
import pytz
   
with models.DAG(
    'dummy-DAG',
    description="Dummy for checking",
    start_date=pendulum.datetime(2024, 9, 30, tz="Asia/Jakarta"),
    schedule_interval='* 1 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['templated', 'TESTING'],
) as dag:
    
    bash_task = BashOperator(
    task_id="bash_task",
    bash_command='echo "Hi from bash operator"')

    bash_task