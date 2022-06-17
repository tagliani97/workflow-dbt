from .models.flag import FlagControl
from datetime import datetime


class Auxiliar:

    @staticmethod
    def get_start(table):
        date = datetime.now().strftime('%Y-%m-%d')
        result = DynamoDB().scan_table_all_pages('octagon-light-Metadata-dev')
        for i in result:
            if table in i["table"]:
                if 'SUCCEEDED' not in i['status_process']:
                    raise Exception(f"{table} não possui status de sucesso")
                elif date not in i['timestamp_finished']:
                    raise Exception(f"{table} não possui status para data atual")

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
