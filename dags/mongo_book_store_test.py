from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def query_books():
    mongo_hook = MongoHook(conn_id='mongo_default')
    books_cursor = mongo_hook.find(
        mongo_collection='books',
        query={},
        projection={'_id': 1},  # Include only the _id field
        limit=10
    )
    books = [str(book['_id']) for book in books_cursor]  # Convert cursor to a list and serialize _id
    for book in books:
        print(f"Book ID: {book}")
    return books

with DAG(
    dag_id='mongo_book_store_test',
    start_date=datetime(2025, 10, 21),
    schedule=None,
    catchup=False,
    tags=['mongo', 'test']
) as dag:

    fetch_books = PythonOperator(
        task_id='fetch_books',
        python_callable=query_books
    )
