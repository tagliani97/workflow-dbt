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

    tb_dyn = ['tb_camp_oferta_pool', 'tb_dyn_sell_incidents']

    Stage(
        dag.dag_id,
        bash_cmd,
        tb_dyn,
        'docker exec -i  image_airflow_dbt_dbt-container_1 bash -c',
        ''
    ).create_stage_task()
