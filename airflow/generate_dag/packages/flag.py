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
                cur.fetchall()
        except Exception as e:
            raise("Problema na execucao da query", e)
        cur.close()
        conn.close()

    def query_by_flag(self, template_type):

        date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        query_dict = {
            'failed': f"""INSERT INTO flag_airflow.airflow_dag_status VALUES ('{self.dag_id}', 'failed', '{date}')""",
            'stage': f"""INSERT INTO flag_airflow.airflow_dag_status VALUES ('{self.dag_id}','success', '{date}')""",
            'tru': f"""
                SELECT dag_id, status, MAX(data_execution) data_success\
                FROM flag_airflow.airflow_dag_status\
                WHERE 1=1\
                and dag_id = '{self.dag_id}'\
                and status = 'success'\
                and data_execution > (\
                    SELECT MAX(data_execution) data_failed\
                    FROM flag_airflow.airflow_dag_status\
                    WHERE 1=1\
                    and dag_id = '{self.dag_id}'\
                    and status = 'failed')\
                and data_execution <= CURRENT_TIMESTAMP AT TIME ZONE 'UTC'\
                GROUP BY dag_id, status;
                """
            }

        result = [str(v.strip()) for k, v in query_dict.items() if template_type == k][0]
        return result
