import psycopg2
from airflow.hooks.base_hook import BaseHook  # Deprecated in Airflow 2


class Database():

    @staticmethod
    def postgress_conection():

        connection = BaseHook.get_connection("dbt_postgres_instance_raw_data")
        conn_args = dict(
            host=connection.host,
            user=connection.login,
            password=connection.password,
            dbname=connection.schema,
            port=connection.port
        )
        conn = psycopg2.connect(**conn_args)
        cur = conn.cursor()
        return cur, conn

    @staticmethod
    def postgres_query(psd_arg, query):

        cur = psd_arg[0]
        conn = psd_arg[1]
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print("Problema na execucao da query", e)
        cur.close()
        conn.close()
