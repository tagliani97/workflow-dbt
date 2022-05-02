import json
import os
import yaml
from yaml.loader import SafeLoader
from dag import Control


class Airflow:

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

    def control_dag(self):

        json_conf, yml_conf = self.read_conf()

        Control(json_conf, yml_conf).create_dag()


config_filepath = 'config.yml'
Airflow(config_filepath).control_dag()
