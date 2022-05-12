from utils.dag import Control
from utils.conf_yaml import ConfigPath


class Airflow:

    def __init__(
        self,
        json_conf,
        yml_conf
    ):
        self.json_conf = json_conf
        self.yml_conf = yml_conf

    def control_dag(self):

        for kwargs in self.json_conf.values():
            if kwargs.get("delete_dag"):
                Control.delete_dag(kwargs.get("dag_id"), **yml_conf)
            else:
                Control.create_dag(self.yml_conf, **kwargs)


if __name__ == '__main__':
    json_conf, yml_conf = ConfigPath.configuration_files()
    Airflow(json_conf, yml_conf).control_dag()