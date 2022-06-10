from datetime import datetime, timezone
from config.database import Database


class FlagControl:

    @staticmethod
    def postgres_query(query):

        psd_arg = Database.postgress_conection()
        cur = psd_arg[0]
        conn = psd_arg[1]
        try:
            if 'INSERT' not in str(query):
                cur.execute(query)
                conn.commit()
                result = str(cur.fetchall()[0])
                if 'success' not in result:
                    raise Exception("Falha na ultima execucao stage")
            else:
                cur.execute(query)
                conn.commit()
        except Exception as e:
            raise(e)
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def query_by_flag(template_type):

        date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        query_dict = {
            'failed': f"""\
                INSERT INTO flag_airflow.airflow_dag_status\
                    VALUES ('dag_by_param', 'failed', '{date}')""",
            'stage': f"""\
                INSERT INTO flag_airflow.airflow_dag_status\
                    VALUES ('dag_by_param','success', '{date}')""",
            'tru': f"""\
                with flag as (\
                    SELECT dag_id, status, MAX(data_execution) data_ref\
                    FROM flag_airflow.airflow_dag_status\
                    WHERE 1=1\
                    and dag_id = 'dag_by_param'\
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
