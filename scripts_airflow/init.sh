#!/usr/bin/env bash

# Setup DB Connection String
AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
export AIRFLOW__CORE__SQL_ALCHEMY_CONN
AIRFLOW__WEBSERVER__SECRET_KEY="openssl rand -hex 30"
export AIRFLOW__WEBSERVER__SECRET_KEY
DBT_POSTGRESQL_CONN="postgresql+psycopg2://${DBT_POSTGRES_USER}:${DBT_POSTGRES_PASSWORD}@${DBT_POSTGRES_HOST}:${POSTGRES_PORT}/${DBT_POSTGRES_DB}"
rm -f /airflow/airflow-webserver.pid
airflow upgradedb
airflow connections --add --conn_id 'dbt_postgres_instance_raw_data' --conn_uri $DBT_POSTGRESQL_CONN

airflow scheduler & airflow webserver

cd /dbt && dbt compile

sleep 10
sleep 10
