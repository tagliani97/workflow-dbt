from airflow.operators import python_operator
from airflow.operators.bash_operator import BashOperator
import sys

sys.path.insert(0, '/opt/generate_dag/core')


from .models.generate import Gen
from .models.operators import Operator
from .models.utils import Auxiliar
from .models.flag import FlagControl


class Task:

    def __init__(
        self,
        dag_id,
        bsh_dict,
        py_dict,
        table_dynamo,
        docker_yml_cmd,
        dbt_yml_path
    ):
        self.dag_id = dag_id
        self.bsh_dict = bsh_dict
        self.py_dict = py_dict
        self.table_dynamo = table_dynamo

        self.inter_eval = lambda x: [eval(v) for v in x.values()]
        self.operators = Operator.auxiliar_op()
        self.init_generator = Gen(
            docker_yml_cmd,
            dbt_yml_path
        )

    def task_tree(self, bash_list, first_task=None):

        trigger_datalake = self.inter_eval(self.init_generator.generate(
                self.operators,
                'trigger_datalake',
                table=self.table_dynamo
            ))

        if first_task is None:
            first_task = trigger_datalake

        try:
            for i in range(0, len(bash_list)):
                if i == 0:
                    first_task >> bash_list[i]
                if i not in [0]:
                    bash_list[i-1] >> bash_list[i]
        except Exception as e:
            raise("Erro na geração da task", e)