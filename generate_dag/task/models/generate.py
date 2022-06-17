
class Gen:

    def __init__(self, docker_yml_cmd, dbt_yml_path):
        self.docker_yml_cmd = docker_yml_cmd
        self.dbt_yml_path = dbt_yml_path

    def generate(
        self,
        operators,
        type_op,
        dicts=None,
        dag_id=None,
        query=None,
        table=None
    ):
        replace_dict = {
            'flag_operator':
                lambda operator, key, value: operator
                .replace('task_paramater', key)
                .replace(
                    'psd_query',
                    query.replace(
                        'dag_by_param',
                        value if 'tru' in dag_id else dag_id
                    )
                ),
            'dbt_operator':
                lambda operator, key, value: operator
                .replace('task_paramater', key)
                .replace(
                    "task_cmmd",
                    f'{self.docker_yml_cmd} "cd {self.dbt_yml_path} ; dbt deps ; {value} "'
                ),
            'trigger_datalake': lambda operator: operator.replace("table_param", table)
        }

        operator = [
            v for k, v in operators.items() if type_op == k
        ][0]

        if dicts:
            operator_replace = lambda key, value: [
                v(operator, key, value) 
                for k, v in replace_dict.items() if k in type_op
            ][0]
        
            task_depends = {
                key: (operator_replace(key, value))
                for key, value in dicts.items()
            }
        else:
            operator_replace = lambda operator: [
                v(operator) 
                for k, v in replace_dict.items() if k in type_op
            ][0]
            task_depends = {}
            task_depends[type_op if type_op in replace_dict.keys() else ''] = operator_replace(operator)

        to_list = lambda x: [v for v in x.values()]

        return to_list(task_depends)
