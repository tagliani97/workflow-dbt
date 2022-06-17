from airflow.models.baseoperator import BaseOperator
import boto3
import os
from datetime import datetime

ACCOUNT_ID = os.getenv('ACCOUNT_ID')
ACCOUNT_REGION = os.getenv('ACCOUNT_REGION')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
ID_ROLE = os.getenv('ROLE')
SNS_NAME = os.getenv('SNS_NAME')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
LOG_DY_APP = os.getenv('APPLICATION')
DAG_PREFIX = os.getenv('DAG_PREFIX')

# NOME DA DAG (QUE VAI APARECER NA INTERFACE DO AIRFLOW)
NOME_DA_DAG = "rd_dl_bi_rd_dw_dim_tp_entrega"

# DAG_PREFIX = Variable.get('DAG_PREFIX')
AWS_CONN = 'aws_default'

# JOB GLUE (NOME DO GLUE JOB DISPONÍVEL NA AWS)
GLUE_JOB_NAME = "rd_dl_bi_rd_dw_dim_tp_entrega"

# GERAÇÃO DO NOME DA DAG DE ACORDO COM O JOB GLUE E A DATA DE EXECUÇÃO
EXEC_DATE = datetime.now()
# EXEC_DATE = x.strftime("%Y-%m-%d %H:%M")

# EVENT_LOG_ID = f'{DAG_PREFIX} DAG - INGESTION -
# {GLUE_JOB_NAME} - {datetime.now()}'

# AIRFLOW VARIABLE - TAGS QUE SERÃO EXIBIDAS NA UI DO AIRFLOW
TAG = ["rd_analytics", "glue job"]

# VARIÁVEIS PARA INSERÇÃO NO DYNAMO (LOG)
LOG_DY_DTS = f"DAG - INGESTION - {GLUE_JOB_NAME}"
LOG_DY_SOURCE = "GLUE JOB"


class Observability(BaseOperator):

    def __init__(self, name: str, **kwargs) -> None:
        # super().__init__(**kwargs)
        self.name = name
        # self.dag = name['dag']
        # self.task = name['task_instance']
        # print("---------------------------")
        print(self.name)
        # print(self.dag)
        # print(self.task)
        print("verificar aqui a estrutura")
        # self.message = message

    def send_log(self):
        context = self.name
        dag = context['dag']
        task = context['task_instance']
        print(dag, task)
        if "start" in str(context['task_instance']):
            print("inicio projeto")
            print(type(context))
            self.message_start_dag()
        elif "finished" in str(context['task_instance']):
            print("final projeto")
            self.message_finished_dag()
        else:
            print("demais tasks")
            self.message_task()

    def message_task(self):
        print("menssagem referente a task")
        print("teste")
        # print(self.name)
        # return "menssagem referente a task"

    def message_start_dag(self):
        print("menssagem referente ao inicio da dag")

    def message_finished_dag(self):
        print("menssagem referente ao fim da dag")

    def msg(self, strTipoMsg, strMsg):
        # f'{DAG_PREFIX}-DAG - INGESTION - {GLUE_JOB_NAME} - {datetime.now()}'
        EVENT_LOG_ID = 'airflow-teste'
        t = str(strTipoMsg)
        m = str(strMsg)
        item = {
            "event":
            {
                "application": LOG_DY_APP,
                "dataset": LOG_DY_DTS,
                "event_date": str(datetime.now()),
                "event_log_id": EVENT_LOG_ID,
                "message": m,
                "source": LOG_DY_SOURCE,
                "status": t
            },
            "event_log_id": f'{EVENT_LOG_ID}'
        }
        return item

    #  CRIA O OBJETO PARA CONECTAR AO DYNAMO
    def client_boto3(
      self,
      service,
      key_id=AWS_ACCESS_KEY_ID,
      access_key=AWS_SECRET_ACCESS_KEY,
      region_name=ACCOUNT_REGION
    ):
        client = boto3.resource(f'{service}',
                                aws_access_key_id=key_id,
                                aws_secret_access_key=access_key,
                                region_name=region_name)
        return client

    #  CRIA O REGISTRO DE SUCESSO PARA SER INSERIDO
    #  NO DYNAMO PELO OPERATOR DYNAMO_LOG
    def status(self, context):
        try:
            # RECUPERA INFORMAÇÃO DA TASK
            str_msg = str(context)  # ['task_instance'])
            if "success" == str(context):  # in str(context['task_instance']):
                type_msg = "Success"
            else:
                str_msg += "," + str(context['exception'])
                type_msg = "Error"
            client = self.client_boto3('dynamodb')
            client = client.Table('event_log')
            response = client.put_item(Item=self.msg(type_msg, str_msg))
            print(response)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    context = 'success'
    run = Observability(context)
    data = run.message_task()  # context)
    print(data)
