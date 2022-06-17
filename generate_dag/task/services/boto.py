import os
import boto3


class InitService:

    AWS_ACCOUNT_REGION = os.getenv('AWS_ACCOUNT_REGION')
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')

    @classmethod
    def client_boto3(cls, service, type_boto3):

        dict = {
                "resource": boto3.resource(
                    f'{service}',
                    aws_access_key_id=cls.AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=cls.AWS_SECRET_ACCESS_KEY,
                    aws_session_token=cls.AWS_SESSION_TOKEN,
                    region_name=cls.AWS_ACCOUNT_REGION),

                "client": boto3.client(
                    f'{service}',
                    aws_access_key_id=cls.AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=cls.AWS_SECRET_ACCESS_KEY,
                    aws_session_token=cls.AWS_SESSION_TOKEN,
                    region_name=cls.AWS_ACCOUNT_REGION)
            }

        result = [
            v
            for k, v in dict.items() if type_boto3 == k
        ][0]

        return result
