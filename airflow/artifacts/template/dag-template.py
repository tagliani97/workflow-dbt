from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,8,1),
    'retries': 0
}

with DAG(
    'dag_json_dag_id',
    default_args=default_args,
    schedule_interval='dag_json_schedule'
) as dag:

    task_1 = BashOperator(
        task_id='daily_transform',
        bash_command='cd /dbt && dbt run dbt_json_command',
        dag=dag
    )

task_1
