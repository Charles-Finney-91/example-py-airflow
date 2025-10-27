from airflow import DAG
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests

def test_http_connectivity(**kwargs):
    conn_id = "rest-book-status-updater"
    base_conn = BaseHook.get_connection(conn_id)
    base_url = f"{base_conn.schema}://{base_conn.host}:{base_conn.port}/"
    print(f"[INFO] Testing HTTP connection to: {base_url}")

    # Perform a test GET request (use a safe endpoint if available)
    try:
        response = requests.get(base_url, headers={"Accept": "application/json"})
        print(f"[INFO] HTTP response status: {response.status_code}")
        print(f"[INFO] Response body (first 200 chars): {response.text[:200]}")
    except Exception as e:
        print(f"[ERROR] Failed to connect: {e}")
        raise


with DAG(
    dag_id="test_http_connection_dag",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=["test", "http", "connectivity"],
) as dag:

    test_http = PythonOperator(
        task_id="test_http_connectivity",
        python_callable=test_http_connectivity,
    )
