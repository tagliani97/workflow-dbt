import shutil
import fileinput
import os


class Control:

    def __init__(self, yml_conf, kwargs):
        self.yml_conf = yml_conf
        self.kwargs = kwargs

    def dict_control(self):

        yml_path = self.yml_conf['dbt_path']
        dag_id = self.kwargs.get("dag_id")
        dag_schedule = self.kwargs.get("dag_schedule")
        dependence = self.kwargs.get("airflow-task")
        task_cmmd = dependence.get("task-command")
        task_name = dependence.get("task-name")

        dict = {
            "dag_json_dag_id": dag_id,
            "dag_json_schedule": dag_schedule,
            "dbt_yml_path": yml_path,
            "deps_bash_cmd": "{0}".format(task_cmmd),
            "deps_names": "{0}".format(task_name)
        }

        return dict

    def create_dag(self, dict_replace):

        airflow_dag_path = self.yml_conf['airflow_dag_path']
        template_path = self.yml_conf['template_path']
        dag_id = self.kwargs.get("dag_id")
        template_name = self.kwargs.get("template-name")
        edit_template = self.kwargs.get("edit_template")

        new_filename = "{0}/{1}.py".format(
            airflow_dag_path,
            dag_id
        )

        if edit_template is False:
            try:
                template_file = "{0}{1}.py".format(
                    template_path,
                    template_name
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

        dag_id = self.kwargs.get("dag_id")
        docker_delete_cmd = self.yml_conf["docker_delete_command"]
        path_dags_container = self.yml_conf["repositorio"]
        path_logs = self.yml_conf["dags_logs"]
        try:
            os.system(f'{docker_delete_cmd} \
                "cd {path_dags_container} ; airflow dags delete -y {dag_id}"')
            os.system(f'{docker_delete_cmd} \
                "cd {path_dags_container} ; rm -r {dag_id}.py"')
            os.system(f'{docker_delete_cmd} \
                "cd {path_logs} ; rm -r {dag_id}/"')
        except Exception as e:
            print("Falha ao deletar a dag", e)


