from airflow.operators import python_operator
from airflow.operators.bash_operator import BashOperator
import sys

sys.path.insert(0, '/opt/generate_dag/core')


from .models.generate import Gen
from .models.operators import Operator
from .models.utils import Auxiliar
from .models.flag import PostgresFlag


class Task:

    def __init__(
        self,
        table_dynamo,
        docker_yml_cmd,
        dbt_yml_path
    ):

        self.table_dynamo = table_dynamo
        self.operators = Operator.auxiliar_op()
        self.init_generator = Gen(
            docker_yml_cmd,
            dbt_yml_path
        )

    def find_task_output(self, object: dict) -> None:
        list_airflow_operator = []
        for v in object.values():
            list_airflow_operator.append(eval(v))
        return list_airflow_operator

    def create_task_tree(
        self,
        dbt_operator_list: list,
        first_task: list = None
    ):

        trigger_datalake = self.find_task_output(self.init_generator.generate(
                self.operators,
                'trigger_datalake',
                table=self.table_dynamo
            ))

        if first_task is None:
            first_task = trigger_datalake

        try:
            for i in range(0, len(dbt_operator_list)):
                if i == 0:
                    first_task >> dbt_operator_list[i]
                if i not in [0]:
                    dbt_operator_list[i-1] >> dbt_operator_list[i]
        except Exception as e:
            raise("Erro na geração da task", e)
