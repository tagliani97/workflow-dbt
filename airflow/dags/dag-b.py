import sys
from airflow import DAG
from airflow.utils.dates import days_ago

sys.path.insert(0, '/airflow/generate_dag')

from packages.task import Task

with DAG(
        dag_id='dag-b',
        schedule_interval='@daily',
        start_date=days_ago(0),
        tags=['test3'],
        catchup=False) as dag:


    bash_cmd = {'task-dbt-1': 'echo teste'}
    python_cmd = {'verifica-status-dag-teste': 'dag-teste'}

    Task(dag.dag_id, 'tru',
                     bash_cmd,
                     python_cmd,
                     '',
                     '/dbt').create_task()
