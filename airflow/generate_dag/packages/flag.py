from datetime import datetime, timezone
from config.database import Database


class FlagControl:

    def __init__(self, dag_id):
        self.dag_id = dag_id

    @staticmethod
    def postgres_query(query):

        psd_arg = Database.postgress_conection()
        cur = psd_arg[0]
        conn = psd_arg[1]
        try:
            cur.execute(query)
            conn.commit()
            if 'INSERT' not in query:
                cur.execute(query)
                conn.commit()
                result = cur.fetchall()
                if 'failed' in result[0]:
                    raise Exception("Falha na ultima execucao")
        except Exception as e:
            raise(e)
        cur.close()
        conn.close()

    def query_by_flag(self, template_type):

        date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        query_dict = {
            'failed': f"""INSERT INTO flag_airflow.airflow_dag_status VALUES ('{self.dag_id}', 'failed', '{date}')""",
            'stage': f"""INSERT INTO flag_airflow.airflow_dag_status VALUES ('{self.dag_id}','success', '{date}')""",
            'tru': f"""
                with flag as (\
                    SELECT dag_id, status, MAX(data_execution) data_ref\
                    FROM flag_airflow.airflow_dag_status\
                    WHERE 1=1\
                    and dag_id = {self.dag_id}\
                    and (status = 'success' or status = ' failed')\
                    and data_execution <= CURRENT_TIMESTAMP AT TIME ZONE 'UTC'\
                    group by dag_id, status\
                )\
                SELECT status FROM flag;
                """
            }

        result = [str(v.strip()) for k, v in query_dict.items() if template_type == k][0]
        return result
