from boto import InitService


class DynamoDB:

    def __init__(self):
        self.client = InitService.client_boto3('dynamodb', "resource")

    def scan_table_all_pages(self, table):
        items = []
        tabledb = self.client.Table(table)
        response = tabledb.scan()
        if len(response) != 0:
            items += response['Items']
            while response.get('LastEvaluatedKey', False):
                response = tabledb.scan(
                    AttributesToGet=['table'],
                    ExclusiveStartKey=response['LastEvaluatedKey'],
                    Select='SPECIFIC_ATTRIBUTES'
                )
                items += response['Items']

        return items
