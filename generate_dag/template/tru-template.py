import sys
from airflow import DAG
from datetime import datetime, timedelta

sys.path.append('/opt/airflow/generate_dag')

from task.tru import Tru

args={
    'owner': 'dag_json_owner',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=args,
    dag_id="dag_json_dag_id",
    start_date=datetime(dag_json_start_date) - timedelta(days=1),
    schedule_interval="dag_json_schedule",
    tags=dag_json_dag_tag,
    catchup=False
) as dag:

    bash_cmd = dict_json_bash
    python_cmd = dict_json_flag
    docker_exec = "docker_yml_cmd"
    docker_dbt_path = "dbt_yml_path"

    Tru(
        bash_cmd,
        docker_exec,
        docker_dbt_path,
        python_cmd,
        dag.dag_id
    ).create_tru_task()

