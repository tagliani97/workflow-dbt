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
            op_kwargs={'dag_id': 'task_cmmd'})'''

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
    date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    return f"""INSERT INTO flag_airflow.airflow_dag_status VALUES ('{dag_id}','success', '{date}')"""


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
        cur.execute(mount_query(kwargs['dag_id']))
        conn.commit()
    except Exception as e:
        print("Problema ao inserir registro Postgres", e)
    cur.close()
    conn.close()


with DAG(
        dag_id='stage-teste-template',
        schedule_interval=None,
        start_date=days_ago(0),
        tags=['stage'],
        catchup=False) as dag:


    bash_cmds = {'task-dbt-1': 'echo teste', 'task-dbt-2': 'echo opa'}
    python_cmds = {'insert_data': dag.dag_id}

    dict_python_operator = generate_dict_by_cmd(
        python_cmds,
        operators('python_operator')
    )

    dict_bash_operator = generate_dict_by_cmd(
        bash_cmds,
        operators('bash_operator')
    )

    start = BashOperator(
        task_id="start",
        bash_command='echo comecei',
    )
    end = BashOperator(
        task_id="end",
        bash_command='echo finalizei',
    )

    operator_inter = lambda x: [eval(v) for v in x.values()]
    python_list = operator_inter(dict_python_operator)
    bash_list = operator_inter(dict_bash_operator)
    bash_list.append(python_list)
    bash_list.append(end)
    for i in range(0, len(bash_list)):
        if i == 0:
            start >> bash_list[i]
        if i not in [0]:
            bash_list[i-1] >> bash_list[i]
