import psycopg2
from airflow import DAG
from datetime import datetime, timezone
from airflow.utils.dates import days_ago
from airflow.operators import python_operator
from airflow.operators.bash_operator import BashOperator


def operators(type_op):
    if type_op == 'python_operator':

        operator = '''python_operator.PythonOperator(
            task_id='task_paramater',
            python_callable=postgres_query,
            op_kwargs={'dag_id': 'task_cmmd', 'status': 'successs'})'''

    elif type_op == 'bash_operator':

        operator = '''BashOperator(
            task_id="task_paramater",
            bash_command='task_cmmd',
            on_success_callback='A',
            on_failure_callback='S')'''

    return operator


def generate_dict_by_cmd(dict, operator):
    task_depends = {}
    for key, value in dict.items():
        value = operator\
            .replace("task_cmmd", value)\
            .replace('task_paramater', key)

        task_depends[key] = value

    return task_depends


def mount_query(dag_id: str) -> str:

    return f"""SELECT dag_id, status, MAX(data_execution)
            FROM flag_airflow.airflow_dag_status
            WHERE 1=1
            and dag_id = '{dag_id}'
            and status = 'success'
            and data_execution <= CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
            GROUP BY dag_id, status;"""


def postgres_query(**kwargs: dict) -> None:
    conn_args = dict(
        host='172.18.0.4',
        user='dbtuser',
        password='pssd',
        dbname='dbtdb',
        port=5432
    )
    conn = psycopg2.connect(**conn_args)
    cur = conn.cursor()
    try:
        print(mount_query(kwargs['dag_id']))
        cur.execute(mount_query(kwargs['dag_id']))
        db_records = cur.fetchall()
        if not db_records:
            raise Exception("Registro não existente para data atual")
        else:
            print(db_records[0])
    except Exception as e:
        raise ("Erro ao procurar registro", e)
    cur.close()
    conn.close()


with DAG(
        dag_id='tru-teste-template',
        schedule_interval='@daily',
        start_date=days_ago(0),
        tags=['stage'],
        catchup=False) as dag:

    operator_inter = lambda x: [eval(v) for v in x.values()]
    bash_cmds = {'task-dbt-1': 'echo teste', 'task-dbt-2': 'echo opa'}
    python_cmds = {'busca-stage_produto': 'stage-produto'}

    dict_python_operator = generate_dict_by_cmd(
        python_cmds,
        operators('python_operator')
    )

    dict_bash_operator = generate_dict_by_cmd(
        bash_cmds,
        operators('bash_operator')
    )

    python_list = operator_inter(dict_python_operator)
    bash_list = operator_inter(dict_bash_operator)

    start = BashOperator(
        task_id="start",
        bash_command='echo comecei',
    )

    end = BashOperator(
        task_id="end",
        bash_command='echo finalizei',
    )

    bash_list.append(end)

    for i in range(0, len(bash_list)):
        if i == 0:
            start >> python_list >> bash_list[i]
        elif i not in [0]:
            bash_list[i-1] >> bash_list[i]
