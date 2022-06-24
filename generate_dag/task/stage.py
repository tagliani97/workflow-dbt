
from .operators import Operator
from .flag import PostgresFlag
from .models.task import Task


class Stage:

    def __init__(
        self,
        dag_id,
        dbt_dict,
        table_dynamo_list,
        docker_yml_cmd,
        dbt_yml_path
    ):

        self.dag_id = dag_id
        self.dbt_dict = dbt_dict
        self.table_dynamo_list = table_dynamo_list
        self.docker_yml_cmd = docker_yml_cmd,
        self.dbt_yml_path = dbt_yml_path
        self.template_type = 'stage'
        self.operators = Operator.auxiliar_op()

    def create_stage_task(self) -> list:

        query = PostgresFlag.type_postgres_query(
            self.template_type,
            self.dag_id
        )

        flag_dict = {'insert_data': query}

        bashl = []
        for k, v in self.operators.items():
            if k == 'dbt_operator':
                for j, l in self.dbt_dict.items():
                    bashl.append(
                        v(
                            j,
                            "{0} 'cd {1} ; dbt deps ; {2}".format(
                                self.docker_yml_cmd[0], self.dbt_yml_path, l)
                            )
                        )

        triiger = []
        for k, v in self.operators.items():
            if k == 'trigger_datalake':
                triiger.append(v(self.table_dynamo_list))

        flagl = []

        for k, v in self.operators.items():
            if k == 'flag_operator':
                for j, l in flag_dict.items():
                    flagl.append(
                        v(
                            j,
                            l
                        )
                    )

        bashl.append(flagl)
        value = Task.create_task_tree(bashl, triiger)
        return value
