import time
from boto3.dynamodb.conditions import Attr
from datetime import datetime, timedelta
from .boto import InitService


class DynamoDB:

    def __init__(self):
        self.client = InitService.type_service_boto3('dynamodb', "resource")
        self.ssm = InitService.type_service_boto3("ssm", "client")
        self.env = self.ssm.get_parameter(
            Name="/config/data-environment",
            WithDecryption=False
        )

    def table_scan_list(self, table_dynamo_list) -> None:

        attr_name = 'table'
        table = f'octagon-light-Metadata-{self.env}'
        tabledb = self.client.Table(table)

        convert_to_data = lambda x : datetime.strptime(x, "%Y-%m-%d %H:%M:%S:%f")
        convert_data_to_str = lambda x : x.strftime("%Y-%m-%d")
        subtract_day = lambda x: x - timedelta(days=0)

        data = {}
        index_status = 0
        for table_dynamo in table_dynamo_list:

            response = tabledb.scan(
                FilterExpression=Attr(str(attr_name)).contains(
                    str(table_dynamo)
                )
            )

            try:
                if not len(response["Items"]):
                    raise Exception(
                        f"Tabela informada não existe {table_dynamo}"
                    )

                for i in response["Items"]:
                    index_status += 1
                    if "SUCCEEDED" in i['status_job'] or "FAILED" in i['status_job']:
                        data[
                            i['status_job'],
                            index_status] = convert_to_data(
                                i["timestamp_finished"]
                            )

                    job_time = max(data.values())
                    max_value = [max(data, key=data.get), job_time]
                    day_result = convert_data_to_str(subtract_day(job_time))
                    date_now = convert_data_to_str(
                            subtract_day(
                                datetime.now()
                            )
                        )

                    assert ("SUCCEEDED" not in max_value),\
                        "Não há registros de sucesso tabela {0} \
                        em seu ultimo processamento".format(table_dynamo)

                    assert (date_now not in day_result),\
                        "Não há registros da {0} na data atual {1}"\
                        .format(table_dynamo, date_now)

                print(table_dynamo, "-> OK")

            except Exception as e:
                print(table_dynamo, "-> FAIL")
                raise e
                break

            finally:
                time.sleep(3)


        return "Sucesso"