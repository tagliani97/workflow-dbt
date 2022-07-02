from ..config.logs import Logger
from ..config.airflow_conn import Connection
from datetime import datetime, timezone
import time

_LOG = Logger()


class PostgresFlag:

    @staticmethod
    def execute_postgres_query(query: str) -> None:

        _LOG.info(" ".join(query.split()))
        cur, conn = Connection.postgress_conection()
        try:
            if "INSERT" not in query:
                cur.execute(query)
                result = eval(str(cur.fetchall()[0]).replace("datetime.", ""))
                msg = "STAGE STATUS -> (STATUS: {0} DATA: {1})".format(
                    result[0],
                    datetime.strftime(result[1], '%Y-%m-%d %H:%M:%S')
                )
                _LOG.debug(msg)
                if "success" not in result[0]:
                    raise ValueError("FALHA NA ULTIMA EXECUCAO STAGE")
                conn.commit()
            else:
                cur.execute(query)
                conn.commit()
        except (Exception, ValueError) as e:
            raise(e)
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def type_postgres_query(template_type: str, dag_id) -> str:

        date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        postgres_sch = 'flag_airflow'
        postgres_table = 'airflow_dag_status'

        query_dict = {
            "failed": f"""\
                INSERT INTO {postgres_sch}.{postgres_table}\
                    VALUES ('{dag_id}' , 'failed', '{date}')""",
            "stage": f"""\
                INSERT INTO {postgres_sch}.{postgres_table}\
                    VALUES ('{dag_id}','success', '{date}')""",
            "tru": f"""\
                with flag as (\
                    SELECT dag_id, status, MAX(data_execution) data_ref\
                    FROM {postgres_sch}.{postgres_table}\
                    WHERE 1=1\
                    and dag_id = '{dag_id}'\
                    and (status = 'success' or status = 'failed')\
                    and data_execution <= CURRENT_TIMESTAMP AT TIME ZONE 'UTC'\
                    group by dag_id, status\
                )\
                SELECT status, data_ref FROM flag\
                WHERE data_ref = (SELECT max(data_ref) FROM flag)
                """
            }

        result = [
            str(v.strip())
            for k, v in query_dict.items() if template_type == k
        ][0]

        return result
