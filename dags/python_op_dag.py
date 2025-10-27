from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='python_operator_example_dag',
    default_args=default_args,
    description='A simple DAG with PythonOperator',
    schedule='@daily',
    start_date=datetime(2025, 10, 21),
    catchup=False,
) as dag:
    
    def get_data(ti):
        ti.xcom_push(key='first_name', value='Charles')
        ti.xcom_push(key='last_name', value='Finney')

    def print_hello(ti):
        first_name = ti.xcom_pull(task_ids='get_data', key="first_name")
        last_name = ti.xcom_pull(task_ids='get_data', key="last_name")
        print(f"Hello, my name is {first_name} {last_name}")

    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello
    )

    data_task = PythonOperator(
        task_id='get_data',
        python_callable=get_data
    )

    data_task >> hello_task