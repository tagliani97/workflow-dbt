from dag.artifacts.configuration import ConfigPath
from dag.crud import CrudDag


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
            init_control = CrudDag(self.yml_conf, kwargs)
            if kwargs.get("delete-dag"):
                init_control.delete_dag()
            else:
                init_control.validation()
                dict_replace = init_control.dict_control()
                init_control.create_dag(dict_replace)


if __name__ == '__main__':
    json_conf, yml_conf = ConfigPath.configuration_files()
    Airflow(json_conf, yml_conf).control_dag()
