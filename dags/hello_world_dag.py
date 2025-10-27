from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def print_message(**kwargs):
    conf = kwargs["dag_run"].conf or {}
    msg = conf.get("message", "No message received")
    print(f"Hello World DAG received message: {msg}")

with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2025, 10, 27),
    schedule=None,
    catchup=False,
    tags=["example", "hello"],
) as dag:

    hello = PythonOperator(
        task_id="print_message",
        python_callable=print_message,
    )
