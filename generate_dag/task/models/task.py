from .operators import Operator
from .flag import PostgresFlag


class Task:

    def __init__(self, dbt_dict, docker_yml_cmd, dbt_yml_path):
        self.dbt_dict = dbt_dict
        self.docker_yml_cmd = docker_yml_cmd,
        self.dbt_yml_path = dbt_yml_path,
        self.operators = Operator.auxiliar_op()

    def postgres_query_id(self, template_type, dag_id):

        query = PostgresFlag.type_postgres_query(
            template_type,
            dag_id
        )

        return query

    def create_insert_success_task(self, template_type, dag_id):

        flag_insert_default = {'insert_success_data': self.postgres_query_id(
            template_type,
            dag_id
        )}

        flag_task = [
            v(j, l) for j, l in flag_insert_default.items()
            for k, v in self.operators.items()
            if k == 'flag_operator'
        ]

        return flag_task

    def create_dbt_task(self):

        dbt_task = []
        for k, v in self.operators.items():
            if k == 'dbt_operator':
                for j, l in self.dbt_dict.items():
                    dbt_task.append(
                        v(
                            j,
                            "{0} 'cd {1} ; dbt deps ; {2}' ".format(
                                self.docker_yml_cmd[0],
                                self.dbt_yml_path[0], l)
                            )
                        )
        return dbt_task

    def create_task_tree(
        self,
        dbt_operator_list: list,
        first_task: list
    ):

        try:
            for i in range(0, len(dbt_operator_list)):
                if i == 0:
                    first_task >> dbt_operator_list[i]
                if i not in [0]:
                    dbt_operator_list[i-1] >> dbt_operator_list[i]
        except Exception as e:
            raise("Erro na geração da task", e)
