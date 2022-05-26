import boto3
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators import python_operator
from airflow.utils.dates import days_ago
# from airflow.models import Variable

# bucket = Variable.get("bucket")
# prefix = Variable.get("prefix")
# file_type = Variable.get("file_type")
# profile = Variable.get("profile")

bucket = "raiadrogasil-datalake-dev-us-east-1-109196921142-external-base"
prefix = "tru_regional"
file_type = 'parquet'
profile = 'default'


def verify_file(
        profile,
        bucket,
        prefix,
        file_type):

    session = boto3.Session()
    s3_client = session.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in pages:
        for obj in page['Contents']:
            key = obj['Key']
            try:
                if file_type in key:
                    print(key)
            except Exception as e:
                print("Arquivo não existe", e)


with models.DAG(
        'stage_venda',
        schedule_interval="*/1 * * * *",
        start_date=days_ago(0),
        tags=['stage'],
        catchup=False) as dag:

    # file_verify = python_operator.PythonOperator(
    #     task_id='hello',
    #     python_callable=verify_file,
    #     op_kwargs={
    #         'profile': profile,
    #         'bucket': bucket,
    #         'prefix': prefix,
    #         'file_type': file_type
    #     })

    task = BashOperator(
            task_id="stage_venda",
            bash_command="time sleep 5",
        )

    task
