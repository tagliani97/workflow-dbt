
from .models.operators import Operator
from .models.flag import PostgresFlag
from .task import Task


class Stage:

    def __init__(
        self,
        dag_id,
        dbt_dict,
        table_dynamo,
        docker_yml_cmd,
        dbt_yml_path
    ):

        self.dag_id = dag_id
        self.dbt_dict = dbt_dict
        self.table_dynamo = table_dynamo
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
                            l
                            # f'{self.docker_yml_cmd} "cd {self.dbt_yml_path} ; dbt deps ; {l} "'
                        )
                    )

        triiger = []
        for k, v in self.operators.items():
            if k == 'trigger_datalake':
                triiger.append(v(self.table_dynamo))

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
