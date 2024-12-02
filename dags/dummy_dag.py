import pendulum

from airflow import models
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def hello_function():
    print("A in Airflow is Awesome!")

with models.DAG(
    'dummy-DAG',
    description="Dummy for checking",
    start_date=pendulum.datetime(2024, 9, 30, tz="Asia/Jakarta"),
    schedule_interval='* 1 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['templated', 'TESTING'],
) as dag:
    
    start_task = BashOperator(
        task_id="start_task",
        bash_command='echo "========================== DAG Start =========================="'
    )

    print_hello = PythonOperator(
        task_id='print_hello',
        python_callable=hello_function,
        provide_context=True
    )

    end_task = BashOperator(
    task_id="end_task",
        bash_command='echo "========================== DAG Finish =========================="'
    )

    start_task >> print_hello >> end_task