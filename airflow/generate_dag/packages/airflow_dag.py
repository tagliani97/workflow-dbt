import shutil
import fileinput
import os
import sys


class Control:

    def __init__(self, yml_conf, kwargs):
        self.yml_conf = yml_conf
        self.kwargs = kwargs
        self.dag_id = kwargs.get("dag-id")
        self.dag_schedule = kwargs.get("dag-schedule")
        self.edit_template = kwargs.get("edit-template")
        self.template_name = kwargs.get("template-name")
        self.bash_task = kwargs.get("bash-task")
        self.flag_task = kwargs.get("flag-task")

    @property
    def flag_task(self):
        return self._flag_task

    @flag_task.setter
    def flag_task(self, value):
        if 'tru' in self.kwargs.get("template-name"):
            if 'flag-task' not in self.kwargs:
                raise Exception("Flag-task não existente")
            else:
                value = self.kwargs.get("flag-task")
        self._flag_task = value

    @property
    def bash_task(self):
        return self._bash_task

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
            "template-name",
        ]
        validate = [i for i in self.kwargs.keys() if i in params_required]
        if len(validate) < len(params_required):
            result = list(set(params_required) - set(validate))
            raise Exception("Parametro obrigatório não especificado", result)

    def dict_control(self):

        yml_path = self.yml_conf['dbt_path']

        dict = {
            "dbt_yml_path": yml_path,
            "dag_json_dag_id": self.dag_id,
            "dag_json_schedule": self.dag_schedule,
            "dict_json_bash": "{0}".format(self.bash_task),
            "dict_json_flag": "{0}".format(self.flag_task)
        }

        return dict

    def create_dag(self, dict_replace):

        airflow_dag_path = self.yml_conf['airflow_dag_path']
        template_path = self.yml_conf['template_path']

        new_filename = "{0}/{1}.py".format(
            airflow_dag_path,
            self.dag_id
        )

        if self.edit_template is False:
            try:
                template_file = "{0}{1}.py".format(
                    template_path,
                    self.template_name
                )
                shutil.copyfile(
                    template_file,
                    new_filename
                )
            except Exception as e:
                print("Erro ao copiar arquivo de template", e)

        for line in fileinput.input(new_filename, inplace=True):
            for r in dict_replace.items():
                line = line.replace(*r)
            print(line, end="")

    def delete_dag(self):

        docker_delete_cmd = self.yml_conf["docker_delete_command"]
        path_dags_container = self.yml_conf["repositorio"]
        path_logs = self.yml_conf["dags_logs"]

        try:
            os.system(f'{docker_delete_cmd} \
                "cd {path_dags_container} ; airflow dags delete -y \
                    {self.dag_id}"')
            os.system(f'{docker_delete_cmd} \
                "cd {path_dags_container} ; rm -r {self.dag_id}.py"')
            os.system(f'{docker_delete_cmd} \
                "cd {path_logs} ; rm -r {self.dag_id}/"')
        except Exception as e:
            print("Falha ao deletar a dag", e)

