
class Gen:

    @staticmethod
    def operator(type_op, dicts, query=''):

        operator_dict = {
            'python_operator': '''python_operator.PythonOperator(
                task_id='task_paramater',
                python_callable=FlagControl.query_by_flag,
                op_kwargs={'flag': "psd_query"},
                on_success_callback=Task.status,
                on_failure_callback=Task.status)''',

            'bash_operator': '''BashOperator(
                task_id="task_paramater",
                bash_command='task_cmmd',
                on_success_callback=Task.status,
                on_failure_callback=Task.status)'''
        }

        result = [v for k, v in operator_dict.items() if type_op == k][0]

        task_depends = {
            key: (
                result.replace("task_cmmd", value)
                .replace('task_paramater', key)
                .replace('psd_query', query))
            for key, value in dicts.items()}

        return task_depends
