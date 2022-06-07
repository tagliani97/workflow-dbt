import sys
from airflow import DAG
from airflow.utils.dates import days_ago

sys.path.insert(0, '/airflow/generate_dag')

from packages.task import Task

with DAG(
        dag_id='dag-stage',
        schedule_interval='@daily',
        start_date=days_ago(0),
        tags=['stage'],
        catchup=False) as dag:


    bash_cmd = {'task-dbt-1': 'ddecho teste', 'task-dbt-2': 'ddecho testde'}
    python_cmd = None
    Task(dag.dag_id, 'stage', bash_cmd, python_cmd).create_task()