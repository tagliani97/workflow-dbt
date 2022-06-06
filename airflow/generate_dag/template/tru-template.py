import sys
from airflow import DAG
from airflow.utils.dates import days_ago

sys.path.insert(0, '/airflow/generate_dag')

from packages.generate import GenOperator

with DAG(
        dag_id='dag_json_dag_id',
        schedule_interval='dag_json_schedule',
        start_date=days_ago(0),
        tags=['stage'],
        catchup=False) as dag:


    bash_cmd = dict_json_bash
    python_cmd = dict_json_flag

    GenOperator(dag.dag_id).execution_tru(bash_cmd, python_cmd)

