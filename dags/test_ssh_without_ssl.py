from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 21),
    'retries': 1,
}

with DAG(
    dag_id='test_ssh_without_ssl',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    test_ssh = SSHOperator(
        task_id='test_ssh',
        ssh_conn_id='ssh_default',
        command='echo "SSH connection successful!"',
    )
