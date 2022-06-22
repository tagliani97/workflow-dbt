import os
import boto3


class InitService:

    AWS_ACCESS_KEY_ID="ASIARS3FEZE3GPW44BJQ"
    AWS_SECRET_ACCESS_KEY="XwqQ+vYKb5d7WHBJZVlTVDbQV5vWQcuZfQB0S730"
    AWS_SESSION_TOKEN="IQoJb3JpZ2luX2VjEEsaCXVzLWVhc3QtMSJFMEMCIDfeC6UPe43CTj/oOvaPZi0D2ydQiom18Yo1Kx/C7t1uAh83ko9xLlUheNRCScqFbgNCz0D5RQAG4ji3z0PZflU8KqUDCGMQARoMMTA5MTk2OTIxMTQyIgxEia4BrfgSf55BCKAqggMHQHYtJ+AMZSuZr9IWadVq+UaoE8D/Nesz+OURQdyWlgIq2jKdEu75t4tRQ1WOsJAgMTNe6jVO1vCz/Z92VxpSpc2mUNf8mnoUZNu58N8cIAxx9SmQqhEOxmx65e0bOa5ptmOQSKgRdGym+EndkYwmtUzMW05Wsj8i4J3loHrbfDH089HPSvL3j8hB4NN3MB6f4ecWgixn78bIrCpX5pG+kU0aagPgFN8izh/uLUXPijv/pdOigkZl1lmtczNqmVxGgK05bWUtc1g/f6rFAV8Ld/YZauNeA5GArmqxJ3vAFsHLpUduxjrjjFLvTnNP17Yl3h+gJd0nAR89vOgTMl/uy65guQ/OJ4//hZa/qA+fbNzPE84xTuZw2POwVTC8NAU6Ugz9TUIF9YFvSjqU07g/QHJhA+4gVSV4n0awrkMDfvAEmUP0qa148O4LciY6QG768eJeHBng24Zs6En/z82WLLzH2/Uu1WlEqQjQyh3VeiduQZmOsYFiZRpYPBDpiknWLDDsuM2VBjqoAd6+zKamAIYCH37a5A9hqVe/BiVT3DbXtTwQ02vngDGol2eAjax3rhAqly18xLMNw2I02J8z8AmTY3Nyl3i0FOt4kfIhhSQiji+gJF9ady2RuyJC7sda/fld+ZBl2XOuNm1sM3VcFrTlYXiDdhwLw/Ca1jc7uFqcmGn+QY+rX7kR7fKaYxMQ+7lA6KJhmex/8tewRQMHWcK94XJekBeOLnpmpONAC1SXAg=="
    AWS_ACCOUNT_REGION="us-east-1"

    @classmethod
    def type_service_boto3(cls, service: str, type_boto3: str) -> str:

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
