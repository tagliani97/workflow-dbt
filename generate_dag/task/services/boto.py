import os
import boto3


class InitService:

    AWS_ACCESS_KEY_ID="ASIARS3FEZE3F647EIXD"
    AWS_SECRET_ACCESS_KEY="w5r6F86bqDSitvg7WIxbixmaiOCMjFt0InJ9LVfu"
    AWS_SESSION_TOKEN="IQoJb3JpZ2luX2VjENL//////////wEaCXVzLWVhc3QtMSJHMEUCIQDYQmUe0C/4LeuYSmv4CNKrPLQQ405y2h9iujAED5tSfwIgNgulKLvY3C4ca0+XyzPtWVu4eBqFbo4PPSZ+++9y5TYqrgMI2///////////ARABGgwxMDkxOTY5MjExNDIiDAHXj765c+2fmYagRiqCA0GR6JU7oOZZll0+mW8VEAQx48XctU1P5EoTngsigt/VqRQtIfP/ucq/gWDUJGMPThcFour9397ZEQGon40/fJDFARiYLXdVif0mH9DOhZuF6y5oVLiVj60YurhsVwDZIjREwdqJv943s6crECGrv5ZCefwggUY6VI9E/F0SdCB9tUowJSVV7nBdD08h3gpYAS5Wl4xlQeHQXuL0tbpn4ZtqzvpQDOaVNywQILWj4xif5AvBlLquL4mGuUpa5oKLPZwBCUF9YBj3qi9ZwYYPFDNCUIeMT/g0BDfQoHqgi67isZHz9qOlzWbtfcN05LmaOdavIoumf3uMTtVcI+Naum2+8tk4Y5oyuDwOHdwqoGQ6dvIdUfMMHLm7Bx6EReXgJH1yYrxmmP01zQyJLC8Js+Y8mBcpSirgVlDdw6xcJp/enOUaKdc2bQ9+oArvvztdZZdlEjo5AeMgF+gnyMbYCbHkvQpnrxDCg9ArvI3S296z581TAh/3QAQv0jCft2FqhJ1YMJb2spUGOqYBnysAHPcQAqatkeEQzaaHpILyR9ZHE+yPLw8rFZANSwLLTK2uYn1Vhc+yXBXNwtD8zzXHz316A/jKYgJKgWXQGWixy0DhR6Cj1KZ14Gxrok1nvkemwvi9PDfzVKteH0ZuEA0eikDu8JVX05WGvUPqGHk/AH79J7fLbl7ob1IaHHM+Xo6Coejwsm/n2rz+2q5CfMb3/fXNQC44OFqUdZI5JHuN7UWOgQ=="
    AWS_ACCOUNT_REGION="us-east-1"
    
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
