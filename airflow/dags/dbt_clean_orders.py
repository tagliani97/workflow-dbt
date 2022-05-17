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
    'dbt_clean_orders',
    default_args=default_args,
    schedule_interval='@daily'
) as dag:

    list_depen_cmd = ['ipconfig', 'echo teste']
    list_depen_names = ['dag_1', 'dag2']
    zip_iterator = zip(list_depen_names, list_depen_cmd)
    a_dictionary = dict(zip_iterator)
    bash_cmmd = "cd /dbt"
    previous_task = None

    for key, value in a_dictionary.items():
        task = BashOperator(
            task_id=f'{key}',
            bash_command=f'{bash_cmmd} {value}',
        )
        if previous_task is not None:
            previous_task >> task
        else:
            previous_task = task
