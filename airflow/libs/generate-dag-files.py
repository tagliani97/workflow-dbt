import json
import os
import shutil
import yaml
import fileinput
from yaml.loader import SafeLoader


class GenerateDag:

    def __init__(
        self,
        config_filepath
    ):
        self.config_filepath = config_filepath

    def read_conf(self) -> dict:

        with open(self.config_filepath) as f:
            yml_conf = yaml.load(f, Loader=SafeLoader)

        json_conf = {}
        for filename in os.listdir(yml_conf["config_dag"]):
            if 'json' in filename:
                f = open(yml_conf["config_dag"] + filename)
                json_conf[filename] = json.load(f)

        return json_conf, yml_conf

    def replace_template(self):

        json_conf, yml_conf = self.read_conf()

        for kwargs in json_conf.values():

            dag_id = kwargs.get("dag_id")
            dag_schedule = kwargs.get("dag_schedule")
            dbt_run = kwargs.get("dbt_run")

            new_filename = "{0}/{1}.py".format(
                yml_conf['airflow_dag_path'],
                dag_id
            )

            if kwargs.get("edit_template") is False:
                shutil.copyfile(
                    yml_conf['template_filename'],
                    new_filename
                )

            for line in fileinput.input(new_filename, inplace=True):
                line = line\
                    .replace("dag_json_dag_id", dag_id)\
                    .replace("dag_json_schedule", dag_schedule)\
                    .replace("dbt_json_command", dbt_run)
                print(line, end="")


config_filepath = 'config.yml'
GenerateDag(config_filepath).replace_template()
