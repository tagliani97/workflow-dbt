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
    'dbt_vendas',
    default_args=default_args,
    schedule_interval='@daily'
) as dag:

    task_1 = BashOperator(
        task_id='daily_transform',
        bash_command='cd /dbt && dbt run --models dbt_clean_orders',
        dag=dag
    )

task_1
