from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='catchup_example_dag',
    default_args=default_args,
    description='A simple DAG to demonstrate catchup behavior',
    schedule='@daily',
    start_date=datetime(2025, 10, 15),
    catchup=True
) as dag:
    
    task1 = PythonOperator(
        task_id='task1',
        python_callable=lambda: print("This is task 1")
    )

    task1