from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import json

def log_response(response):
    """Log the response from the REST API."""
    print("Response from Google Search API:")
    print(response)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 21),
    'retries': 1,
}

with DAG(
    dag_id='http_operator_test',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    # Check if the Google Search endpoint is available
    check_google = HttpSensor(
        task_id='check_google',
        http_conn_id='google_https_default',
        endpoint='',
        response_check=lambda response: "Google" in response.text,
        poke_interval=5,
        timeout=20,
    )

    # Call the Google Search REST endpoint
    call_google = HttpOperator(
        task_id='call_google',
        http_conn_id='google_https_default',
        endpoint='search?q=Airflow',
        method='GET',
        response_filter=lambda response: response.text,
        log_response=True,
    )

    # Log the response
    log_result = PythonOperator(
        task_id='log_result',
        python_callable=log_response,
        op_args=["{{ ti.xcom_pull(task_ids='call_google') }}"],
    )

    check_google >> call_google >> log_result
