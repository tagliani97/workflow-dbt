from datetime import datetime

class Control:

    def __init__(self, yml_conf: dict, kwargs: dict):
        self.yml_conf = yml_conf
        self.kwargs = kwargs

    def validation(self) -> None:

        params_required = [
            "owner",
            "dag-id",
            "dag-schedule",
            "dag-tag",
            "template-name",
            "edit-template",
            "start-date",
            "bash-task"
        ]

        validate = [i for i in self.kwargs.keys() if i in params_required]
        result = list(set(params_required) - set(validate))

        if result:
            print(
                "Parametro obrigatório não especificado", result
            )

        validate_str = tuple(filter(
            lambda x: x[0] if x[1] is None or x[1] == '' else '', [
                [k, v] for k, v in self.kwargs.items()]
            ))

        get_list_dict_param = [
            [k, v] for k, v in self.kwargs.items()
            if 'dict' in str(type(v))
        ]

        filter_dict_param = [
            i[0] for i in filter(
                lambda x: x[0] if any(
                    x[1]
                ) is False else '', get_list_dict_param)
        ]

        return validate_str, filter_dict_param

    def param_dict_control(self) -> dict:

        dict = {
            "dag_json_dag_id": self.kwargs.get("dag-id"),
            "dag_json_schedule": self.kwargs.get("dag-schedule"),
            "dag_json_start_date": self.kwargs.get("start-date").replace("-",","),
            "dag_json_owner": self.kwargs.get("owner"), 
            "dbt_yml_path": self.yml_conf['docker_dbt_path'],
            "docker_yml_cmd": "{0}".format(
                self.yml_conf['docker_command']
            ),
            "dag_json_dag_tag": "{0}".format(self.kwargs.get("dag-tag")),
            "dict_json_bash": "{0}".format(self.kwargs.get("bash-task")),
            "dict_json_flag": "{0}".format(self.kwargs.get("flag-task")),
            "dag_json_tb_status_list": "{0}".format(
                self.kwargs.get("table-status")
            )
        }

        return dict
