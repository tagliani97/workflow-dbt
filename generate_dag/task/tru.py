from operators import Operator
from flag import PostgresFlag
from task import Task


class Tru:

    def __init__(
        self,
        dag_id,
        dbt_dict,
        flag_dict,
        docker_yml_cmd,
        dbt_yml_path
    ):

        self.dag_id = dag_id
        self.dbt_dict = dbt_dict
        self.flag_dict = flag_dict
        self.docker_yml_cmd = docker_yml_cmd,
        self.dbt_yml_path = dbt_yml_path
        self.template_type = 'tru'
        self.operators = Operator.auxiliar_op()

    def create_tru_task(self):

        query = lambda x : PostgresFlag.type_postgres_query(
            self.template_type,
            x
        )

        bashl = []
        for k, v in self.operators.items():
            if k == 'dbt_operator':
                for j, l in self.dbt_dict.items():
                    bashl.append(
                        v(
                            j,
                            "{0} 'cd {1} ; dbt deps ; {2} '".format(
                                self.docker_yml_cmd[0], self.dbt_yml_path, l)
                            )
                        )

        flagl = []
        for k, v in self.operators.items():
            if k == 'flag_operator':
                for j, l in self.flag_dict.items():
                    flagl.append(
                        v(
                            j,
                            query(l)
                        )
                    )

        value = Task.create_task_tree(bashl, flagl)
        return value
