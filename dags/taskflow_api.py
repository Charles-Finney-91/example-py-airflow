from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='taskflow_api_example_dag',
    default_args=default_args,
    description='A simple DAG using the TaskFlow API',
    schedule='@daily',
    start_date=datetime(2025, 10, 21)
)
def taskflow_api_example_dag():
    
    @task(multiple_outputs=True)
    def get_data():
        return {
            'first_name': 'Charles',
            'last_name': 'Finney'
        }

    @task()
    def print_hello(first_name, last_name):
        print(f"Hello, my name is {first_name} {last_name}")

    name_dict = get_data()

    print_hello(first_name=name_dict['first_name'], last_name=name_dict['last_name'])

dag = taskflow_api_example_dag()