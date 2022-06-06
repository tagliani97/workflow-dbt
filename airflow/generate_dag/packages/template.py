from airflow.operators import python_operator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timezone
from config.database import Database


class FlagControl():

    def __init__(self, template_type, dag_id):

        self.template_type = template_type
        self.dag_id = dag_id

    def query_by_template(self):

        if 'stage' in self.template_type:
            date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            query_type = f"""
            INSERT INTO flag_airflow.airflow_dag_status\
            VALUES ('{self.dag_id}','success', '{date}')"""

        elif 'tru' in self.template_type:
            query_type = f"""\
            SELECT dag_id, status, MAX(data_execution)\
            FROM flag_airflow.airflow_dag_status\
            WHERE 1=1\
            and dag_id = {self.dag_id} \
            and status = 'success' \
            and data_execution <= CURRENT_TIMESTAMP AT TIME ZONE 'UTC'\
            GROUP BY dag_id, status;"""
        return query_type


class GenOperator(FlagControl):
    def __init__(self, dag_id, template_type):
        super().__init__(template_type, dag_id)

    def generate(self, dict, type_op):
        task_depends = {}
        operator_inter = lambda x: [eval(v) for v in x.values()]
        for key, value in dict.items():
            value = type_op\
                .replace("task_cmmd", value)\
                .replace('task_paramater', key)\
                .replace('psd_query', self.query_by_template().strip())
            task_depends[key] = value
        return operator_inter(task_depends)

    def operators(self, dict, type_op):
        if type_op == 'python_operator':

            operator = '''python_operator.PythonOperator(
                task_id='task_paramater',
                python_callable=Database.postgres_query,
                op_kwargs={'psd_arg': Database.postgress_conection(),\
                'query': "psd_query"})'''

        elif type_op == 'bash_operator':

            operator = '''BashOperator(
                task_id="task_paramater",
                bash_command='task_cmmd',
                on_success_callback='A',
                on_failure_callback='S')'''

        return self.generate(dict, operator)

    def airflow_task(self, bash_list, python_list=None):

        start = BashOperator(
            task_id="start",
            bash_command='echo comecei',
        )

        end = BashOperator(
            task_id="end",
            bash_command='echo finalizei',
        )

        if python_list is None:
            first_task = start
        else:
            first_task = start >> python_list

        bash_list.append(end)

        for i in range(0, len(bash_list)):
            if i == 0:
                first_task >> bash_list[i]
            if i not in [0]:
                bash_list[i-1] >> bash_list[i]

    def execution(self, *args):

        try:
            task_tree = ''
            if self.template_type == 'stage':

                python_dict = {'insert_data': self.dag_id}

                list_task_bash = self.operators(args[0], 'bash_operator')
                list_task_python = self.operators(python_dict, 'python_operator')
                list_task_bash.append(list_task_python)
                task_tree = self.airflow_task(list_task_bash)

            if self.template_type == 'tru':

                list_task_bash = self.operators(args[0], 'bash_operator')
                list_task_python = self.operators(args[1], 'python_operator')
                task_tree = self.airflow_task(list_task_bash, list_task_python)

        except Exception as e:
            raise("Problema na geração do operador", e)
        return task_tree
