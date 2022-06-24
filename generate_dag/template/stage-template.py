import sys
from airflow import DAG
from airflow.utils.dates import days_ago

sys.path.insert(0, '/opt/generate_dag')

from task.stage import Stage

with DAG(
        dag_id='dag_json_dag_id',
        schedule_interval='dag_json_schedule',
        start_date=days_ago(0),
        tags=dag_json_dag_tag,
        catchup=False) as dag:


    bash_cmd = dict_json_bash
    datalake_table_status = dag_json_tb_status_list
    docker_exec = docker_yml_cmd
    docker_dbt_path = "dbt_yml_path"

    Stage(
        dag.dag_id,
        bash_cmd,
        datalake_table_status,
        docker_exec,
        docker_dbt_path
    ).create_stage_task()
