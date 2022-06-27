from .models.task import Task


class Stage(Task):

    def __init__(
        self,
        dbt_dict,
        docker_yml_cmd,
        dbt_yml_path,
        dag_id,
        table_status_list,
    ):
        super().__init__(dbt_dict, docker_yml_cmd, dbt_yml_path)
        self.dag_id = dag_id
        self.table_status_list = table_status_list
        self.template_type = 'stage'

    def create_stage_task(self) -> list:

        flag_dict = {'insert_data': self.postgres_query_id(
            self.template_type,
            self.dag_id
        )}
        status_datalake = [
            v(self.table_status_list)
            for k, v in self.operators.items()
            if k == 'trigger_datalake'
        ]
        flag_task = [
            v(j, l) for j, l in flag_dict.items()
            for k, v in self.operators.items()
            if k == 'flag_operator'
        ]
        dbt_task = self.create_dbt_task()
        dbt_task.append(flag_task)
        value = self.create_task_tree(dbt_task, status_datalake)
        return value
