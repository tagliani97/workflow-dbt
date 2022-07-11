import sys
from airflow import DAG
from datetime import datetime, timedelta

sys.path.append('/opt/airflow/generate_dag')

from task.tru import Tru

args={
    'owner': 'tagliani',
    'retries': 3,
}

with DAG(
    default_args=args,
    dag_id="asd-tru",
    start_date=datetime(2022,7,11) - timedelta(days=1),
    schedule_interval="15 10 * * 1-5",
    tags=['example'],
    catchup=False
) as dag:

    bash_cmd = {'task-dbt-1': 'echo teste'}
    python_cmd = {'verifica_nf_trusted': 'dag_nf_trusted'}
    docker_exec = "docker exec -i  image_airflow_dbt_dbt-container_1 bash -c"
    docker_dbt_path = "/opt/airflow/dbt/"

    Tru(
        bash_cmd,
        docker_exec,
        docker_dbt_path,
        python_cmd,
        dag.dag_id
    ).create_tru_task()

