import sys
from airflow import DAG
from airflow.utils.dates import days_ago

sys.path.append('/opt/generate_dag')

from task.tru import Tru

with DAG(
        dag_id='asd-tru',
        schedule_interval='@daily',
        start_date=days_ago(0),
        tags=['example'],
        catchup=False) as dag:

    bash_cmd = {'task-dbt-1': 'echo teste'}
    python_cmd = {'verifica_nf_trusted': 'dag_nf_trusted'}
    docker_exec = "docker exec -i  image_airflow_dbt_dbt-container_1 bash -c"
    docker_dbt_path = "/opt/airflow/dbt/"

    Tru(
        bash_cmd,
        docker_exec,
        docker_dbt_path,
        python_cmd
    ).create_tru_task()

