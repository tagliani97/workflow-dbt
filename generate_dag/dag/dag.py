

class Control:

    def __init__(self, yml_conf, kwargs):
        self.yml_conf = yml_conf
        self.kwargs = kwargs
        self.bash_task = None
        self.flag_task = None

    @property
    def flag_task(self):
        return self._flag_task

    @property
    def bash_task(self):
        return self._bash_task

    @flag_task.setter
    def flag_task(self, value):
        if 'tru' in self.kwargs.get("template-name"):
            if 'flag-task' not in self.kwargs:
                raise Exception("Flag-task não existente")
            else:
                value = self.kwargs.get("flag-task")
        self._flag_task = value

    @bash_task.setter
    def bash_task(self, value):
        if 'bash-task' not in self.kwargs:
            raise Exception("Flag-task não existente")
        else:
            value = self.kwargs.get("bash-task")
        self._bash_task = value

    def validation(self):

        params_required = [
            "dag-id",
            "dag-schedule",
            "dag-tag",
            "template-name"
        ]
        validate = [i for i in self.kwargs.keys() if i in params_required]
        if len(validate) < len(params_required):
            result = list(set(params_required) - set(validate))
            raise Exception("Parametro obrigatório não especificado", result)

    def dict_control(self):

        dict = {
            "dag_json_dag_id": self.kwargs.get("dag-id"),
            "dag_json_schedule": self.kwargs.get("dag-schedule"),
            "dbt_yml_path": self.yml_conf['docker_dbt_path'],
            "docker_yml_cmd": self.yml_conf['docker_command'],
            "dag_json_dag_tag": "{0}".format(self.kwargs.get("dag-tag")),
            "dict_json_bash": "{0}".format(self.bash_task),
            "dict_json_flag": "{0}".format(self.flag_task),
            "dag_json_tb_dynamo": "{0}".format(
                self.kwargs.get("table-dynamo")
            )
        }

        return dict
