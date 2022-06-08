import sys
from airflow import DAG
from airflow.utils.dates import days_ago

sys.path.insert(0, '/airflow/generate_dag')

from packages.task import Task

with DAG(
        dag_id='dag-new',
        schedule_interval='@daily',
        start_date=days_ago(0),
        tags=['example'],
        catchup=False) as dag:


    bash_cmd = {'task-dbt-1': 'echo teste', 'task-dbt-2': 'echo teste', 'task-dbt-3': 'echo teste', 'task-dbt-4': 'echo teste', 'task-dbt-5': 'echo teste'}
    python_cmd = None

    Task(dag.dag_id, 'stage',
                     bash_cmd,
                     python_cmd,
                     '',
                     '').create_task()
