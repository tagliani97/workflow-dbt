import sys
from airflow import DAG
from airflow.utils.dates import days_ago

sys.path.append('/opt/generate_dag')

from task.stage import Stage

with DAG(
        dag_id='asd',
        schedule_interval='@daily',
        start_date=days_ago(0),
        tags=['example'],
        catchup=False) as dag:


    bash_cmd = {'task-dbt-1': 'echo teste'}
    datalake_table_status = []
    docker_exec = "docker exec -i  image_airflow_dbt_dbt-container_1 bash -c"
    docker_dbt_path = "/opt/airflow/dbt/"

    Stage(
        bash_cmd,
        docker_exec,
        docker_dbt_path,
        dag.dag_id,
        datalake_table_status
    ).create_stage_task()
