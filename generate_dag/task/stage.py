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

    def create_stage_task(self) -> list:

        insert_data = self.create_insert_success_task(self.dag_id)

        status_datalake = [
            v(self.table_status_list)
            for k, v in self.operators.items()
            if k == 'trigger_datalake'
        ]

        dbt_task = self.create_dbt_task()
        dbt_task.append(insert_data)
        value = self.create_task_tree(dbt_task, status_datalake)
        return value
