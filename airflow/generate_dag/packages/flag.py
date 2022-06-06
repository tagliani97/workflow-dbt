from datetime import datetime, timezone


class FlagControl:

    def __init__(self, dag_id):
        self.dag_id = dag_id

    def query_stage(self):
        date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        query = f"""INSERT INTO flag_airflow.airflow_dag_status VALUES ('{self.dag_id}','success', '{date}')"""
        return query

    def query_tru(self):
        query = f"""SELECT dag_id, status, MAX(data_execution) FROM flag_airflow.airflow_dag_status WHERE 1=1 and dag_id = {self.dag_id} and status = 'success' and data_execution <= CURRENT_TIMESTAMP AT TIME ZONE 'UTC' GROUP BY dag_id, status;"""
        return query
