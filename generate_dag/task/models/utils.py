from models.flag import PostgresFlag
from datetime import datetime

import sys
import json
sys.path.insert(0, "/opt/generate_dag/task")

from services.dynamo import DynamoDB


class Auxiliar:

    @staticmethod
    def collect_status_datalake(table: str) -> None:
        print('table dynamodb ->', table)
        # DynamoDB().scan_table(table)
        pass

    @staticmethod
    def task_status(context: dict) -> dict:
        # run = Observability(context)
        dag_id = str(context['dag']).split()[1].replace('>', '')
        if 'failed' in str(context['task_instance']):
            query = PostgresFlag.type_postgres_query("failed")\
                .replace('dag_by_param', dag_id)
            print([query])
            PostgresFlag.execute_postgres_query(query)
            print('Flag failed postgres')
        return context
