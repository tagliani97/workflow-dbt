from abc import abstractmethod
from airflow.operators import python_operator
from airflow.operators.bash_operator import BashOperator
from flag import FlagControl
from generate import Gen


class AuxiliarOperator:

    @staticmethod
    def auxiliar_op():

        start = BashOperator(
            task_id="start",
            bash_command='echo comecei',
            on_success_callback=Task.status,
            on_failure_callback=Task.status
        )
        end = BashOperator(
            task_id="end",
            bash_command='echo finalizei',
            on_success_callback=Task.status,
            on_failure_callback=Task.status
        )

        return (start, end)


class Task(FlagControl):

    def __init__(self, dag_id, template_type, bsh_dict, py_dict):
        super().__init__(dag_id)
        self.template_type = template_type
        self.bsh_dict = bsh_dict
        self.py_dict = py_dict
        self.inter_eval = lambda x: [eval(v) for v in x.values()]
        self.auxiliar_task = AuxiliarOperator.auxiliar_op()

    @staticmethod
    def status(context):
        return 'rodei'

    def task_tree(self, bash_list, first_task=None):
        if first_task is None:
            first_task = self.auxiliar_task[0]
        bash_list.append(self.auxiliar_task[1])
        try:
            for i in range(0, len(bash_list)):
                if i == 0:
                    first_task >> bash_list[i]
                if i not in [0]:
                    bash_list[i-1] >> bash_list[i]
        except Exception as e:
            raise("Erro na geração da task", e)

    def stage_list(self, template):

        python_dict = {'insert_data': self.dag_id}
        bash_list = self.inter_eval(Gen.operator('bash_operator', self.bsh_dict))
        py_list = self.inter_eval(Gen.operator('python_operator', python_dict, self.query_by_flag(template)))
        bash_list.append(py_list)
        value = self.task_tree(bash_list)
        return value

    def tru_list(self, template):

        bash_list = self.inter_eval(Gen.operator('bash_operator', self.bsh_dict))
        py_list = self.inter_eval(Gen.operator('python_operator', self.py_dict, self.query_tru(template)))
        first_task = self.auxiliar_task[0] >> py_list
        value = self.task_tree(bash_list, first_task)
        return value

    def create_task(self):

        if 'stage' in self.template_type:
            return self.stage_list(self.template_type)

        if 'tru' in self.template_type:
            return self.stage_list(self.template_type)
