from .models.flag import PostgresFlag
from .task import Task


class Stage(Task):

    def __init__(
        self,
        dag_id,
        dbt_dict,
        table_dynamo,
        docker_yml_cmd,
        dbt_yml_path
    ):
        super().__init__(
            table_dynamo,
            docker_yml_cmd,
            dbt_yml_path
        )

        self.dag_id = dag_id
        self.dbt_dict = dbt_dict
        self.template_type = 'stage'

    def create_stage_task(self) -> list:

        flag_dict = {'insert_data': self.dag_id}

        dbt_operator_list = self.find_task_output(self.init_generator.generate(
                self.operators,
                'dbt_operator',
                self.dbt_dict
        ))

        flag_operator_list = self.find_task_output(
            self.init_generator.generate(
                self.operators,
                'flag_operator',
                flag_dict,
                self.dag_id,
                PostgresFlag.type_postgres_query(self.template_type),
                self.table_dynamo
            ))

        dbt_operator_list.append(flag_operator_list)
        value = self.create_task_tree(dbt_operator_list)
        return value
