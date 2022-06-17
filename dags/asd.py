import sys
from airflow import DAG
from airflow.utils.dates import days_ago

sys.path.insert(0, '/opt/generate_dag')

from task.stage import Stage

with DAG(
        dag_id='asd',
        schedule_interval='@daily',
        start_date=days_ago(0),
        tags=['example'],
        catchup=False) as dag:

    bash_cmd = {'task-dbt-1': 'echo teste'}
    python_cmd = None

    table_dynamo = 'tb_crm_categoria'
    docker_cmd = 'docker exec -i  image_airflow_dbt_dbt-container_1 bash -c'
    dbt_path = ''


    Stage(dag.dag_id, bash_cmd, python_cmd, table_dynamo, docker_cmd, dbt_path).create_task()