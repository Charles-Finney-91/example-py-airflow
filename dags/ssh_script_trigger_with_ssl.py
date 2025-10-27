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
    dag_id='ssh_script_trigger_with_ssl',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    execute_script1 = SSHOperator(
        task_id='execute_script1',
        ssh_conn_id='ssh_with_ssl',
        command='bash /scripts/script1.sh ',
    )

    execute_script2 = SSHOperator(
        task_id='execute_script2',
        ssh_conn_id='ssh_default',
        command='bash /scripts/script2.sh ',
    )

    execute_script1 >> execute_script2
