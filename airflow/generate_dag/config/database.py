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