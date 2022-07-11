import sys
from airflow import DAG
from datetime import datetime

sys.path.append('/opt/airflow/generate_dag')

from task.stage import Stage

with DAG(
        dag_id='dag_json_dag_id',
        start_date=datetime(dag_json_start_date),
        schedule_interval='dag_json_schedule',
        tags=dag_json_dag_tag,
        catchup=False) as dag:


    bash_cmd = dict_json_bash
    datalake_table_status = dag_json_tb_status_list
    docker_exec = "docker_yml_cmd"
    docker_dbt_path = "dbt_yml_path"

    Stage(
        bash_cmd,
        docker_exec,
        docker_dbt_path,
        dag.dag_id,
        datalake_table_status
    ).create_stage_task()