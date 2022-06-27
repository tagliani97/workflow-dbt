from .models.task import Task


class Tru(Task):

    def __init__(
        self,
        dbt_dict,
        docker_yml_cmd,
        dbt_yml_path,
        flag_dict
    ):
        super().__init__(dbt_dict, docker_yml_cmd, dbt_yml_path)
        self.flag_dict = flag_dict
        self.template_type = 'tru'

    def create_tru_task(self):

        flag_task = [
            v(
                j, self.postgres_query_id(self.template_type, l)
            ) for j, l in self.flag_dict.items()
            for k, v in self.operators.items()
            if k == 'flag_operator'
        ]

        dbt_task = self.create_dbt_task()
        value = self.create_task_tree(dbt_task, flag_task)
        return value
