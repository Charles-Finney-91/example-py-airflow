from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def test_mongo_connectivity():
    mongo_hook = MongoHook(conn_id='mongo_default') 
    client = mongo_hook.get_conn()
    db_names = client.list_database_names()
    print(f"Successfully connected to MongoDB. Databases: {db_names}")
    return db_names

with DAG(
    dag_id='mongo_test',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['mongo'],
) as dag:

    test_mongo_connection = PythonOperator(
        task_id='test_mongo_connection',
        python_callable=test_mongo_connectivity,
    )
