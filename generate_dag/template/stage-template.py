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
    python_cmd = dict_json_flag

    Stage(
        dag.dag_id,
        bash_cmd,
        python_cmd,
        'docker_yml_cmd',
        'dbt_yml_path',
        'dag_json_tb_dynamo'
    ).create_task()
