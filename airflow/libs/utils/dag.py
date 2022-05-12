import shutil
import fileinput
import os




class Control:

    @staticmethod
    def create_dag(yml_conf, **kwargs):

        dag_id = kwargs.get("dag_id")
        dag_schedule = kwargs.get("dag_schedule")
        dbt_run = kwargs.get("dbt_run")
        template_name = kwargs.get("template-name")
        dependence = kwargs.get("dependence")

        new_filename = "{0}/{1}.py".format(
            yml_conf['airflow_dag_path'],
            dag_id
        )

        if kwargs.get("edit_template") is False:
            try:
                template_file = "{0}{1}.py".format(
                    yml_conf['template_path'],
                    template_name
                )
                shutil.copyfile(
                    template_file,
                    new_filename
                )
            except Exception as e:
                print("Erro ao copiar arquivo de template", e)

        for line in fileinput.input(new_filename, inplace=True):
            for r in (
                ("dag_json_dag_id", dag_id),
                ("dag_json_schedule", dag_schedule),
                ("dbt_run", dbt_run),
                ("dbt_yml_path", yml_conf['dbt_path']),
                ("deps_bash_cmd", "{0}".format(dependence.get("command"))),
                ("deps_names", "{0}".format(dependence.get("names")))
            ):
                line = line.replace(*r)

            print(line, end="")

    @staticmethod
    def delete_dag(dag_id, **yml_conf):

        docker_delete_cmd = yml_conf["docker_delete_command"]
        path_dags_container = '/opt/airflow/dags/rd-da-dw-dbt-etl'
        path_logs = '/opt/airflow/logs'
        try:
            os.system(f'{docker_delete_cmd} \
                "cd {path_dags_container} ; airflow dags delete -y {dag_id}"')
            os.system(f'{docker_delete_cmd} \
                "cd {path_dags_container} ; rm -r {dag_id}.py"')
            os.system(f'{docker_delete_cmd} \
                "cd {path_logs} ; rm -r {dag_id}/"')
        except Exception as e:
            print("Falha ao deletar a dag", e)

