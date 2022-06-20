import os
import boto3


class InitService:

    AWS_ACCESS_KEY_ID="ASIARS3FEZE3D3P6L5KI"
    AWS_SECRET_ACCESS_KEY="jqUqVH05Orb/qNwrkffTRKEQqPYpU6At93JhEZu+"
    AWS_SESSION_TOKEN="IQoJb3JpZ2luX2VjEBcaCXVzLWVhc3QtMSJHMEUCIG3QunhCPtY/GqdWyraytaruoGy33iBRkzCTQP/6io+SAiEA1sKqUXXJWgOrVucvacr81LxB+uxGHxBuyOD3VLxL5HAqpQMIMBABGgwxMDkxOTY5MjExNDIiDF0EI0kE11S3qXHSfyqCA/61b0AdxKldyy1hUWMpe5AOmLWInwG29B9j7IDd3BW4tF7bcl9X95bVrQfHCq4nrwHiJleWU5IesetC70EP+ojP4y4JXkdxWw+fboSDYDj4zd9sLOcvQJNwi7a+z2T2Camy4vA4wz4ZPI2jVN5WJ5OI7/M92JpwYPPTGvR4urtx0AnNzp+XVPv7pfS76l09DxibszydAN7ll1lQXw1seJMW8JSC0fqKW9AFn0MNOqx+/BafICjr4XylT6PZSgKITOaLIDMDiwYZXMcMW60/5sdqmLBbz2MERaDHFflm4FrfpGi9NnD8ZrfHtRwD27Z8NQbNjh+GP/YDNGCs9z+3SM2WrZNmvcJ0z+hMR1XglqvpdtJixylalVldNoumQI9U/RS7uCaNQPcd5m5ZRktPtBRn+wR8Hqz1OUlPQjKP7H9Zn6/q09ZVAB1u4IbrUVFE312YTnnrcltShugy+LTdxZ6nOERcI1I+dn3nHtXhv8KNKM2ZtSIcnfktUr9isgJEga7cMMOGwpUGOqYBvJ5cVqf4CqZtn9w9SKagbTS8eU5ZuSf4CtzamsMb4HvgRQrHz0u1eB7injYUNV79HZrjibfIdmqIABglhxRtQNm0F80Iz8fFJQ/xxm9PO+DwcPH0K8c53iaNAlfNvwAEtLWWX7q1VzY9LOkGUwnbio5GOlXnTsV8iB+QZjtmvHMYX42VfhHNNNifvGgTsCy4yNokzlBtJwhsU99aEkiSovlxDbWCnA=="
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
