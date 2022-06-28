import os
import boto3


class InitService:

    AWS_ACCESS_KEY_ID="ASIARS3FEZE3PWNM7QR3"
    AWS_SECRET_ACCESS_KEY="TWfA98bH+6qSqCvxfhX+WjDsiz8jESyysxrqIWpV"
    AWS_SESSION_TOKEN="IQoJb3JpZ2luX2VjENn//////////wEaCXVzLWVhc3QtMSJHMEUCIGg6QadNHpd3QHy1RCQzEnwZJ3vj3tsE5l/o1LBgpFb+AiEA2YdVsiBnkTxgD2zPnI2+a+RPiuhPM35L98mBUk/KuY8qrgMI8v//////////ARABGgwxMDkxOTY5MjExNDIiDHBedROpFcrhM+u17iqCA9axfxKl208pm7d3Teu5acKUvEEqZax+WVWAIKk8a99riQinlSJqkdupjNRH+D2hwSRdbj9fAr5jZkKTiVBE10VGJRpoPlkyEMm07bj7zTwgMmvot8u/20JwNcUbzA3vKTJe/14tuCPvooiHU0hzjmt65wG71Oi42arwLzXwWNy822MOOdR6igWp49dI0mIz6/Zzr+moiUG53rnMlPEwyCfTsfViooqNEBDPpPnTI3PhfpcLDvm966QflS8o31lHA+PheAxcrGjfWRPKZUN7XJ25JgWJo0/r699uljFdwO0SqGQ/4yx3V7jA/Ir9d+js/3uAt1Oi+eWgTaonRtQ9amySaRZvvoGnbllr8WuPefqHIfLnBORGjZyywElD+VUcTmbtOIcAwP9dvi9yLyIfTzWAxLg45qZk0+PxuJM4YmjFgIm9ruw0EVki2fyIjbubTPqaFMzei9BojG70z55R/cTuLYv5OJYJ+9J6T66Qvts7swdfqfqeueLd3/yztTKU+4gWMJbh7JUGOqYB182Z/qYIYH1T3G/xMqADxERx5zuzsiJ277QNVQ7SyZzTIKmNKxG2oII8prLqkC9CiX+EzyjLMwcw/R0GdCGC0VIofiWBz9wcqWYcVd3jECu+DrIyE/UQRCyeH//w8++fEwp66um+ixjIVLiZNQiibq6z2v4mRTe6e9CrXsG0QOmwFNo7N0WuE6iYkKV1xKS37AIgzFJHKim8W5IwPrQJhKZKJ0K4HA=="
    AWS_ACCOUNT_REGION = "us-east-1"

    # AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    # AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    # AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
    # AWS_ACCOUNT_REGION = "us-east-1"

    @classmethod
    def type_service_boto3(cls, service: str, type_boto3: str) -> str:

        try:
            if "resource" in type_boto3:
                boto = boto3.resource(
                    f'{service}',
                    aws_access_key_id=cls.AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=cls.AWS_SECRET_ACCESS_KEY,
                    aws_session_token=cls.AWS_SESSION_TOKEN,
                    region_name=cls.AWS_ACCOUNT_REGION)
            else:
                boto = boto3.client(
                    f'{service}',
                    aws_access_key_id=cls.AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=cls.AWS_SECRET_ACCESS_KEY,
                    aws_session_token=cls.AWS_SESSION_TOKEN,
                    region_name=cls.AWS_ACCOUNT_REGION)
        except Exception as e:
            raise ValueError("Não foi possivel conectar a aws", e)

        return boto
