from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import threading
import requests
import json
import os


def query_books(**kwargs):
    mongo_hook = MongoHook(conn_id="mongo_default")
    books_cursor = mongo_hook.find(
        mongo_collection="books",
        query={"status": "unread"},
        projection={"_id": 1},
        limit=10,
    )
    books = list(books_cursor)
    if not books:
        return None

    ids_payload = {"ids": [str(book["_id"]) for book in books]}
    kwargs["ti"].xcom_push(key="book_ids", value=ids_payload)
    return ids_payload


def fire_and_forget_post(**kwargs):
    ids_payload = kwargs["ti"].xcom_pull(task_ids="query_books", key="book_ids")
    if not ids_payload:
        return

    http_hook = HttpHook(http_conn_id="rest-book-status-updater", method="PUT")
    base_url = http_hook.base_url.rstrip("/")
    endpoint = "/api/books/update-status"
    full_url = f"{base_url}{endpoint}"

    def send_request():
        try:
            requests.put(full_url, json=ids_payload, timeout=1)
        except Exception:
            pass

    threading.Thread(target=send_request, daemon=True).start()


def trigger_next_dag(**kwargs):
    ids_payload = kwargs["ti"].xcom_pull(task_ids="query_books", key="book_ids")
    if not ids_payload:
        return

    airflow_url = os.getenv("AIRFLOW__WEBSERVER__BASE_URL", "http://localhost:8080")
    endpoint = f"{airflow_url.rstrip('/')}/api/v1/dags/batch_process_watcher_dag/dagRuns"

    headers = {"Content-Type": "application/json"}

    # Optional: Basic Auth (if Airflow API auth is enabled)
    auth = None
    username = os.getenv("AIRFLOW_API_USER")
    password = os.getenv("AIRFLOW_API_PASSWORD")
    if username and password:
        auth = (username, password)

    payload = {
        "conf": ids_payload,
        "dag_run_id": f"watcher_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    }

    response = requests.post(endpoint, headers=headers, auth=auth, json=payload)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to trigger DAG: {response.status_code} {response.text}")


with DAG(
    dag_id="batch_data_server_dag",
    start_date=datetime(2025, 10, 21),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["mongo", "http", "batch"],
) as dag:

    query_task = PythonOperator(
        task_id="query_books",
        python_callable=query_books,
    )

    post_task = PythonOperator(
        task_id="create_json_and_post_async",
        python_callable=fire_and_forget_post,
    )

    trigger_task = PythonOperator(
        task_id="trigger_batch_process_watcher",
        python_callable=trigger_next_dag,
    )

    query_task >> post_task >> trigger_task
