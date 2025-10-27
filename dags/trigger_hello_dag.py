from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="trigger_hello_dag",
    start_date=datetime(2025, 10, 27),
    schedule=None,
    catchup=False,
    tags=["example", "trigger"],
) as dag:

    # Trigger the second DAG and pass configuration as a dictionary
    trigger_second = TriggerDagRunOperator(
        task_id="trigger_second_dag",
        trigger_dag_id="hello_world_dag",
        conf={"message": "Hello from first DAG"},   # ✅ must be a dict, not a Jinja string
        wait_for_completion=False,                  # don’t block
    )
