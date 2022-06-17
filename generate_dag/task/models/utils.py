from models.flag import FlagControl
from datetime import datetime

import sys
import json
sys.path.insert(0, "/opt/generate_dag/task")

from services.dynamo import DynamoDB


class Auxiliar:

    @staticmethod
    def get_start(table):
        print('table dynamodb ->', table)
        DynamoDB().scan_table(table)

    @staticmethod
    def status(context):
        # run = Observability(context)
        dag_id = str(context['dag']).split()[1].replace('>', '')
        if 'failed' in str(context['task_instance']):
            query = FlagControl.query_by_flag("failed")\
                .replace('dag_by_param', dag_id)
            FlagControl.postgres_query(query)
            print('Flag failed postgres')
        return context
