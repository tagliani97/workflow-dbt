from task import Task
from .models.flag import FlagControl


class Tru(Task):

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
            dbt_yml_path,
        )

        self.template_type = 'tru'

    def tru_list(self, template):

        bash_list = self.inter_eval(
            self.init_generator.generate('dbt_operator', self.bsh_dict))

        py_list = self.inter_eval(
            self.init_generator.operator(
                'flag_operator',
                self.py_dict,
                'tru',
                FlagControl.query_by_flag(template)
            )
        )

        first_task = py_list
        value = self.task_tree(bash_list, first_task)
        return value

    def create_task(self):
        self.tru_list(self.template_type)
