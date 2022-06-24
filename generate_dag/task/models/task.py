
class Task:

    @staticmethod
    def create_task_tree(
        dbt_operator_list: list,
        first_task: list
    ):

        try:
            for i in range(0, len(dbt_operator_list)):
                if i == 0:
                    first_task >> dbt_operator_list[i]
                if i not in [0]:
                    dbt_operator_list[i-1] >> dbt_operator_list[i]
        except Exception as e:
            raise("Erro na geração da task", e)
