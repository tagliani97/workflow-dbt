import sys
from airflow import DAG
from datetime import datetime, timedelta

sys.path.append('/opt/airflow/generate_dag')

from task.stage import Stage


args={
    'owner': 'tagliani',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=args,
    dag_id="asd",
    start_date=datetime(2022,7,11) - timedelta(days=1),
    schedule_interval="15 10 * * 1-5",
    tags=['example'],
    catchup=False
) as dag:

    bash_cmd = {'task-dbt-1': 'echo teste'}
    datalake_table_status = []
    docker_exec = "['docker exec -i  image_airflow_dbt_dbt-container_1 bash -c']"
    docker_dbt_path = ""

    Stage(
        bash_cmd,
        docker_exec,
        docker_dbt_path,
        dag.dag_id,
        datalake_table_status
    ).create_stage_task()