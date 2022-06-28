import sys
from airflow import DAG
from airflow.utils.dates import days_ago

sys.path.append('/opt/generate_dag')

from task.tru import Tru

with DAG(
        dag_id='dag_json_dag_id',
        schedule_interval='dag_json_schedule',
        start_date=days_ago(0),
        tags=dag_json_dag_tag,
        catchup=False) as dag:

    bash_cmd = dict_json_bash
    python_cmd = dict_json_flag
    docker_exec = "docker_yml_cmd"
    docker_dbt_path = "dbt_yml_path"

    Tru(
        bash_cmd,
        docker_exec,
        docker_dbt_path,
        python_cmd
    ).create_tru_task()

