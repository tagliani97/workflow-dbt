from .models.flag import FlagControl
from .task import Task


class Stage(Task):

    def __init__(
        self,
        dag_id,
        bsh_dict,
        py_dict,
        table_dynamo,
        docker_yml_cmd,
        dbt_yml_path
    ):
        super().__init__(
            dag_id,
            bsh_dict,
            py_dict,
            table_dynamo,
            docker_yml_cmd,
            dbt_yml_path
        )

        self.template_type = 'stage'

    def stage_list(self, template):

        python_dict = {'insert_data': self.dag_id}

        bash_list = self.inter_eval(self.init_generator.generate(
                self.operators,
                'dbt_operator',
                self.bsh_dict
        ))

        py_list = self.inter_eval(self.init_generator.generate(
                self.operators,
                'flag_operator',
                python_dict,
                self.dag_id,
                FlagControl.query_by_flag(template),
                self.table_dynamo
            ))

        bash_list.append(py_list)
        value = self.task_tree(bash_list)
        return value

    def create_task(self):
        return self.stage_list(self.template_type)
