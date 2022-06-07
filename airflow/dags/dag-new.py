import sys
from airflow import DAG
from airflow.utils.dates import days_ago

sys.path.insert(0, '/airflow/generate_dag')

from packages.task import Task

with DAG(
        dag_id='dag-new',
        schedule_interval='@daily',
        start_date=days_ago(0),
        tags=['stage'],
        catchup=False) as dag:


    bash_cmd = {'task-dbt-1': 'echo teste', 'task-dbt-2': 'echo opa'}
    python_cmd = {'task-flag': 'dag-teste', 'task-flag1': 'dag-teste', 'task-flag4': 'dag-teste'}

    Task(dag.dag_id, 'tru', bash_cmd, python_cmd).create_task()

