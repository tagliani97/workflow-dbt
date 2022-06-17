import shutil
import fileinput

from .dag import Control


class CrudDag(Control):
    def __init__(self, yml_conf, kwargs):
        super().__init__(yml_conf, kwargs)

    def create_dag(self, dict_replace):

        airflow_dag_path = self.yml_conf['airflow_dag_path']
        template_path = self.yml_conf['template_path']

        new_filename = "{0}/{1}.py".format(
            airflow_dag_path,
            self.kwargs.get("dag-id")
        )

        if self.kwargs.get("edit-template") is False:
            try:
                template_file = "{0}{1}.py".format(
                    template_path,
                    self.kwargs.get("template-name")
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
