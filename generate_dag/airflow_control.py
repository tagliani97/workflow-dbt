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
                validate_str, filter_dict = init_control.validation()
                if not validate_str and not filter_dict:
                    dict_replace = init_control.param_dict_control()
                    init_control.create_dag(
                        dict_replace, kwargs.get("dag-id")
                    )


if __name__ == '__main__':
    json_conf, yml_conf = ConfigPath.configuration_files()
    Airflow(json_conf, yml_conf).control_dag()
