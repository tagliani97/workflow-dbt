from boto3.dynamodb.conditions import Attr
from datetime import datetime
from .boto import InitService


class DynamoDB:

    def __init__(self):
        self.client = InitService.type_service_boto3('dynamodb', "resource")

    def scan_table(self, table_dynamo: str) -> None:

        attr_name = 'table'
        table = 'octagon-light-Metadata-dev'
        tabledb = self.client.Table(table)
        try:
            response = tabledb.scan(
                FilterExpression=Attr(str(attr_name)).contains(
                    str(table_dynamo)
                )
            )
            result = [datetime.strptime(
                i['timestamp_finished'], '%Y-%m-%d %H:%M:%S:%f'
            ) for i in response['Items'] if "SUCCEEDED" in i['status_job']]

            assert (len(result) != 0),\
                "Não há registros de sucesso tabela {0}"\
                .format(table_dynamo)

            data_result = max(result)
            day_result = data_result.strftime("%Y-%m-%d")
            date_now = datetime.now().strftime("%Y-%m-%d")

            assert (datetime.now().strftime("%Y-%m-%d") in day_result),\
                "Não há registros da {0} na data atual {1}"\
                .format(table_dynamo, date_now)

        except BaseException as e:
            raise ("Problema scan dynamodb", e)
