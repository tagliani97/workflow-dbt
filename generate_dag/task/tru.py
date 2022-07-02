from .models.task import Task


class Tru(Task):

    def __init__(
        self,
        dbt_dict,
        docker_yml_cmd,
        dbt_yml_path,
        flag_dict,
        dag_id
    ):
        super().__init__(dbt_dict, docker_yml_cmd, dbt_yml_path)
        self.flag_dict = flag_dict
        self.dag_id = dag_id
        self.template_type = 'tru'

    def create_tru_task(self):

        insert_data = self.create_insert_task(self.template_type, self.dag_id)

        flag_task = [
            v(
                j, self.postgres_query_id(self.template_type, l)
            ) for j, l in self.flag_dict.items()
            for k, v in self.operators.items()
            if k == 'flag_operator'
        ]

        dbt_task = self.create_dbt_task()
        dbt_task.append(insert_data)
        value = self.create_task_tree(dbt_task, flag_task)
        return value
