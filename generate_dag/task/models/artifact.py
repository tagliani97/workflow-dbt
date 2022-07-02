from ..config.logs import Logger
from .flag import PostgresFlag

_LOG = Logger()


class Auxiliar:

    @staticmethod
    def task_status(context: dict) -> dict:
        # run = Observability(context)
        dag_id = str(context['dag']).split()[1].replace('>', '')
        if 'failed' in str(context['task_instance']):
            query = PostgresFlag.type_postgres_query("failed", dag_id)
            _LOG.error('DAG FALHOU, INSERIDO FALHA POSTGRES')
            PostgresFlag.execute_postgres_query(query)
        return context
