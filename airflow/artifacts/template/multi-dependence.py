import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0
}

with DAG(
    'dag_json_dag_id',
    default_args=default_args,
    schedule_interval='dag_json_schedule'
) as dag:

    list_depen_cmd = deps_bash_cmd
    list_depen_names = deps_names
    zip_iterator = zip(list_depen_names, list_depen_cmd)
    a_dictionary = dict(zip_iterator)
    previous_task = None

    for key, value in a_dictionary.items():
        task = BashOperator(
            task_id=f'{key}',
            bash_command=f'{value}',
        )
        if previous_task is not None:
            previous_task >> task
        else:
            previous_task = task
