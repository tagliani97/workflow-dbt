
from airflow.operators import python_operator
from airflow.operators.bash_operator import BashOperator

from .services.cloudwatch import CloudWatchLogs
from .flag import PostgresFlag
from .utils import Auxiliar


class Operator:

    @staticmethod
    def auxiliar_op() -> dict:

        operator_dict = {

            "trigger_datalake": lambda value: python_operator.PythonOperator(
                    task_id='datalake-status',
                    python_callable=CloudWatchLogs().control_query_logs,
                    op_kwargs={"table_scan_list": value},
                    on_success_callback=Auxiliar.task_status,
                    on_failure_callback=Auxiliar.task_status),

            "flag_operator": lambda key, value: python_operator.PythonOperator(
                task_id=key,
                python_callable=PostgresFlag.execute_postgres_query,
                op_kwargs={"query": value},
                on_success_callback=Auxiliar.task_status,
                on_failure_callback=Auxiliar.task_status),

            "dbt_operator": lambda key, value: BashOperator(
                task_id=key,
                bash_command=value,
                on_success_callback=Auxiliar.task_status,
                on_failure_callback=Auxiliar.task_status
            ),

        }

        return operator_dict
