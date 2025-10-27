from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='bash_operator_example',
    default_args=default_args,
    description='A simple BashOperator DAG example',
    schedule="@daily",
    start_date=datetime(2025, 10, 21),
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo "This is the first task!"'
    )
    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo "This is the second task!"'
    )
    task1 >> task2