from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def push_function(**context):
    value = "Hello from Task 1!"
    context['ti'].xcom_push(key='message', value=value)
    print("XCom pushed:", value)


def pull_function(**context):
    value = context['ti'].xcom_pull(task_ids='task_push', key='message')
    print("XCom pulled:", value)


with DAG(
    dag_id="xcom_example_dag",
    start_date=datetime(2025, 10, 21),
    schedule=None,
    catchup=False
) as dag:

    task_push = PythonOperator(
        task_id='task_push',
        python_callable=push_function
    )

    task_pull = PythonOperator(
        task_id='task_pull',
        python_callable=pull_function
    )

    task_push >> task_pull
