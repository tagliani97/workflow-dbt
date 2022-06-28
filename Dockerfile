# BUILD: docker build --rm -t airflow-server .
# SOURCE: https://github.com/puckel/docker-airflow

FROM apache/airflow:2.0.2
LABEL maintainer="DataEngineering"

# Never prompt the user for choices on installation/configuration of packages

ENV  DEBIAN_FRONTEND=noninteractive
ENV TERM linux


# Dependencies Airflow
#ADD requirements.txt .



USER root

RUN apt-get update && apt-get install -y git
RUN apt-get install -y python-dev
RUN apt-get install -y libpq-dev


RUN apt-get install python3 && apt-get install -y python3-pip

RUN curl -sSL https://get.docker.com/ | sh

COPY .env /.env



USER airflow
#WORKDIR ${AIRFLOW_USER_HOME}
#ENTRYPOINT ["/entrypoint.sh"]
CMD chmod 666 /var/run/docker.sock
