
class Gen:

    def __init__(self, docker_yml_cmd, dbt_yml_path):
        self.docker_yml_cmd = docker_yml_cmd
        self.dbt_yml_path = dbt_yml_path

    def operator(self, type_op, dicts, query=''):

        operator_dict = {
            'python_operator': '''python_operator.PythonOperator(
                task_id='task_paramater',
                python_callable=FlagControl.postgres_query,
                op_kwargs={'query': "psd_query"})''',

            'bash_operator': '''BashOperator(
                task_id="task_paramater",
                bash_command = 'task_cmmd',
                on_success_callback=Auxiliar.status,
                on_failure_callback=Auxiliar.status)'''
        }

        result = [v for k, v in operator_dict.items() if type_op == k][0]

        task_depends = {
            key: (
                result.replace(
                    "task_cmmd",
                    f"{value}")
                    # f'{self.docker_yml_cmd} "cd {self.dbt_yml_path} ; dbt deps ; {value} "')
                .replace('psd_query', query if query else '')
                .replace('task_paramater', key))
            for key, value in dicts.items()
        }

        return task_depends
