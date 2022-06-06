from flag import FlagControl
from task import Task


class GenOperator(FlagControl):
    def __init__(self, dag_id):
        super().__init__(dag_id)

    @staticmethod
    def operators(type_op):

        if type_op == 'python_operator':

            operator = '''python_operator.PythonOperator(
                task_id='task_paramater',
                python_callable=Database.postgres_query,
                op_kwargs={
                    'psd_arg': Database.postgress_conection(),\
                    'query': "psd_query"
                },
                on_success_callback=Task.status,
                on_failure_callback=Task.status)'''

        elif type_op == 'bash_operator':

            operator = '''BashOperator(
                task_id="task_paramater",
                bash_command='task_cmmd',
                on_success_callback=Task.status,
                on_failure_callback=Task.status)'''

        return operator

    def generate(self, dict, type_op, query=None):
        task_depends = {}
        for key, value in dict.items():
            value = type_op\
                .replace("task_cmmd", value)\
                .replace('task_paramater', key)
            if query:
                value.replace('psd_query', query)
            task_depends[key] = value
            return task_depends

    def execution_stage(self, dict):
        python_dict = {'insert_data': self.dag_id}
        bash_list = self.generate(
            dict,
            self.operators('bash_operator')
        )

        python_list = self.generate(
            python_dict,
            self.operators('python_operator'),
            self.query_stage()
        )

        task_tree = Task.airflow_task(
            bash_list
            , python_list
            , 'stage'
        )
        return task_tree

    def execution_tru(self, *args):
        bash_list = self.generate(
            args[0],
            self.operators('bash_operator')
        )
        python_list = self.generate(
            args[1],
            self.operators('python_operator'),
            self.query_tru()
        )
        task_tree = Task.airflow_task(
            bash_list,
            python_list
            ,'tru'
        )
        return task_tree
