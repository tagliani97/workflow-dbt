import shutil
import fileinput


class Control:

    def __init__(
        self,
        json_conf,
        yml_conf
    ):
        self.json_conf = json_conf
        self.yml_conf = yml_conf

    def create_dag(self):

        for kwargs in self.json_conf.values():

            dag_id = kwargs.get("dag_id")
            dag_schedule = kwargs.get("dag_schedule")
            dbt_run = kwargs.get("dbt_run")

            new_filename = "{0}/{1}.py".format(
                self.yml_conf['airflow_dag_path'],
                dag_id
            )

            if kwargs.get("edit_template") is False:
                try:
                    template_file = "{0}{1}.py".format(
                        self.yml_conf['template_path'],
                        kwargs.get('template-name')
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
                    ("dbt_json_command", dbt_run)
                ):
                    line = line.replace(*r)

                print(line, end="")

        # def delete_dag(self):