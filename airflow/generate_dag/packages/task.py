from airflow.operators import python_operator
from airflow.operators.bash_operator import BashOperator
from config.database import Database


class Task:

    @staticmethod
    def status(context):
        print("Rodei", context)

    @staticmethod
    def airflow_task(bash_list, python_list, type_template):

        inter_eval = lambda x: [eval(v) for v in x.values()]

        start = BashOperator(
            task_id="start",
            bash_command='echo comecei',
            on_success_callback=Task.status,
            on_failure_callback=Task.status
        )

        end = BashOperator(
            task_id="end",
            bash_command='echo finalizei',
            on_success_callback=Task.status,
            on_failure_callback=Task.status
        )

        python_list = inter_eval(python_list)
        bash_list = inter_eval(bash_list)

        if 'stage' in type_template:
            bash_list.append(python_list)
            first_task = start
        if 'tru' in type_template:
            first_task = start >> python_list

        bash_list.append(end)
        for i in range(0, len(bash_list)):
            if i == 0:
                first_task >> bash_list[i]
            if i not in [0]:
                bash_list[i-1] >> bash_list[i]