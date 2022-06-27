import logging
from .flag import PostgresFlag


class Auxiliar:

    @staticmethod
    def task_status(context: dict) -> dict:
        # run = Observability(context)
        dag_id = str(context['dag']).split()[1].replace('>', '')
        if 'failed' in str(context['task_instance']):
            query = PostgresFlag.type_postgres_query("failed", dag_id)
            PostgresFlag.execute_postgres_query(query)
            print('Flag failed postgres')
        return context


class Logs:

    # Create and configure logger
    logging.basicConfig(filename="newfile.log",
                        format='%(asctime)s %(message)s',
                        filemode='w')

    # Creating an object
    logger = logging.getLogger()

    # Setting the threshold of logger to DEBUG
    logger.setLevel(logging.DEBUG)

    # Test messages
    logger.debug("Harmless debug Message")
    logger.info("Just an information")
    logger.warning("Its a Warning")
    logger.error("Did you try to divide by zero")
    logger.critical("Internet is down")