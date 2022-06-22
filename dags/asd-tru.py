import sys
from airflow import DAG
from airflow.utils.dates import days_ago

sys.path.insert(0, '/opt/generate_dag')

from task.tru import Tru

with DAG(
        dag_id='asd-tru',
        schedule_interval='@daily',
        start_date=days_ago(0),
        tags=['example'],
        catchup=False) as dag:

    bash_cmd = {'task-dbt-1': 'echo teste'}
    python_cmd = {'task-flag': 'asd'}

    Tru(
        dag.dag_id,
        bash_cmd,
        python_cmd,
        'docker exec -i  image_airflow_dbt_dbt-container_1 bash -c',
        ''
    ).create_tru_task()
