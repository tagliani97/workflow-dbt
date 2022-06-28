from ..config.logs import Logger
from ..config.boto import InitService
from datetime import datetime, timedelta
import time
import ast
import re


class CloudWatchLogs():

    def __init__(self):
        self.client = InitService.type_service_boto3('logs', "client")
        self.log = Logger()

    def query_logs(self, query, log_group, reductor=0):

        convert_to_data = lambda x : datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
        convert_data_to_str = lambda x : x.strftime("%Y-%m-%d 00:00:00")
        data_q = convert_to_data(convert_data_to_str(datetime.today()))

        data_query = data_q - timedelta(days=reductor)

        start_query_response = self.client.start_query(
            logGroupName=log_group,
            startTime=int((data_query).timestamp()),
            endTime=int(datetime.now().timestamp()),
            queryString=query,
        )

        query_id = start_query_response['queryId']

        response = None

        self.log.debug(f"Procurando status tabela {data_query}")
        while response is None or response['status'] == 'Running':
            time.sleep(reductor)
            response = self.client.get_query_results(
                queryId=query_id
            )

        return response['results']

    def control_query_logs(self, table_scan_list):

        for table in table_scan_list:
            print(table)
            try:
                query = f"""
                    fields \
                        @timestamp, @message, table, status_job, @ingestionTime
                    | filter table='{table.strip()}' \
                    and (status_job="FAILED" or status_job="SUCCEEDED")
                    | sort @timestamp desc
                """

                log_group = '/aws/lambda/sdlf-heavy-control-status'

                time.sleep(1)
                size_logs = None
                reductor = -1

                while size_logs is None or not size_logs:
                    reductor += 1
                    if reductor == 60:
                        raise Exception(
                            f"Tempo limite de {reductor} dias excedido {table}"
                        )
                    size_logs = self.query_logs(query, log_group, reductor)

                max_time = max([logs[0]['value'] for logs in size_logs])

                dict_dataset = None
                for logs in size_logs:
                    if max_time in str(logs):
                        for logs_dict in logs:
                            if 'job_id' in str(logs_dict['value']):
                                dict_dataset = ast.literal_eval(
                                    re.search(
                                        '({.+})', logs_dict['value']
                                    ).group(0)
                                )

                if 'FAILED' in dict_dataset['status_job']:
                    self.log.error(f"{table} -> FAIL")
                    raise Exception("""Ultimo status possui erro,
                                    motivo""", [dict_dataset['error']])

                self.log.info(f"{table} -> OK")
                time.sleep(3)
            except Exception as e:
                raise e

