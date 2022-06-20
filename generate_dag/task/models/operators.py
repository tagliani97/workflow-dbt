
class Operator:

    @staticmethod
    def auxiliar_op() -> dict:

        operator_dict = {

            "trigger_datalake": """python_operator.PythonOperator(
                    task_id='datalake-status',
                    python_callable=Auxiliar.collect_status_datalake,
                    op_kwargs={"table": "table_param"},
                    on_success_callback=Auxiliar.task_status,
                    on_failure_callback=Auxiliar.task_status)""",
            
            "flag_operator": """python_operator.PythonOperator(
                task_id='task_paramater',
                python_callable=PostgresFlag.execute_postgres_query,
                op_kwargs={"query": "psd_query"},
                on_success_callback=Auxiliar.task_status,
                on_failure_callback=Auxiliar.task_status)
            """,
            
            "dbt_operator": """BashOperator(
                task_id="task_paramater",
                bash_command = "task_cmmd",
                on_success_callback=Auxiliar.task_status,
                on_failure_callback=Auxiliar.task_status)
            """,

            "end": """BashOperator(
                task_id="end",
                bash_command='echo finalizei')
            """
        }

        return operator_dict
