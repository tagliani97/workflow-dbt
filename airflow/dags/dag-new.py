import sys
from airflow import DAG
from airflow.utils.dates import days_ago

sys.path.insert(0, '/airflow/generate_dag')

from packages.template import GenOperator

with DAG(
        dag_id='dag-new',
        schedule_interval='@daily',
        start_date=days_ago(0),
        tags=['stage'],
        catchup=False) as dag:


    bash_cmd = {'task-dbt-1': 'echo teste', 'task-dbt-2': 'echo opa'}
    python_cmd = {'task-flag': 'dag-teste'}

    init_stage = GenOperator(dag.dag_id, 'tru')

    init_stage.execution(bash_cmd, python_cmd)

