from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator


with DAG(
        'stage_child',
        schedule_interval="*/1 * * * *",
        start_date=days_ago(0),
        tags=['stage'],
        catchup=False) as dag:

    sensor_stage_venda = ExternalTaskSensor(
        task_id='sensor_stage_venda',
        external_dag_id='stage_venda',
        external_task_id='stage_venda')

    dim_task = BashOperator(
            task_id="dim_vendas",
            bash_command="time sleep 1")

    sensor_stage_venda >> dim_task
