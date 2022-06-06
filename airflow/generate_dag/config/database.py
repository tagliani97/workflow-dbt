import psycopg2


class Database():

    @staticmethod
    def postgress_conection():
        conn_args = dict(
            host='172.18.0.3',
            user='dbtuser',
            password='pssd',
            dbname='dbtdb',
            port=5432
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