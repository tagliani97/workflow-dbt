import json
import os
import shutil
import fileinput

airflow_artifacts_path = '/airflow/artifacts'
airflow_dag_path = '/airflow/dags'

config_filepath = f'{airflow_artifacts_path}/config_json/'
dag_template_filename = f'{airflow_artifacts_path}/template/dag-template.py'


for filename in os.listdir(config_filepath):
    f = open(config_filepath + filename)
    config = json.load(f)
    dag_id = f"{config['DagId']}"
    dag_schedule = config['Schedule']
    dag_command = config['RunCommand']

    new_filename = "{0}/{1}.py".format(airflow_dag_path, dag_id)
    shutil.copyfile(dag_template_filename, new_filename)

    for line in fileinput.input(new_filename, inplace=True):
        line = line\
            .replace("dag_json_dag_id", dag_id)\
            .replace("dag_json_schedule", dag_schedule)\
            .replace("dbt_json_command", dag_command)
        print(line, end="")
