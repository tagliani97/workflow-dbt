from .flag import PostgresFlag


class Auxiliar:

    @staticmethod
    def task_status(context: dict) -> dict:
        # run = Observability(context)
        dag_id = str(context['dag']).split()[1].replace('>', '')
        if 'failed' in str(context['task_instance']):
            query = PostgresFlag.type_postgres_query("failed", dag_id)
            PostgresFlag.execute_postgres_query(query)
            print('Flag failed postgres')
        return context
