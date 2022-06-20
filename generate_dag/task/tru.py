from .models.flag import PostgresFlag
from .task import Task


class Tru(Task):

    def __init__(
        self,
        dag_id,
        dbt_dict,
        flag_dict,
        table_dynamo,
        docker_yml_cmd,
        dbt_yml_path
    ):
        super().__init__(
            table_dynamo,
            docker_yml_cmd,
            dbt_yml_path,
        )

        self.dag_id = dag_id
        self.dbt_dict = dbt_dict
        self.flag_dict = flag_dict
        self.template_type = 'tru'

    def create_tru_task(self):

        dbt_operator_list = self.find_task_output(self.init_generator.generate(
            self.operators,
            'dbt_operator',
            self.dbt_dict
        ))

        flag_operator_list = self.find_task_output(
            self.init_generator.generate(
                self.operators,
                'flag_operator',
                self.flag_dict,
                'tru',
                PostgresFlag.type_postgres_query(self.template_type)
            )
        )

        first_task = flag_operator_list
        value = self.create_task_tree(dbt_operator_list, first_task)
        return value
