build:
	sudo mkdir -p logs
	sudo mkdir -p dags
	sudo mkdir -p dbt
	sudo mkdir -p pluguins
	sudo cp profiles.yml dbt/
	sudo chmod -R 777 logs/ dags/ dbt/ pluguins/ generate_dag/
	sudo docker build . -f Dockerfile --tag airflow_dbt:0.0.1
	sudo docker build . -f Dockerfile-dbt --tag dbt_rd:0.0.1

run:
	sudo docker-compose up airflow-init
	sudo docker-compose up

#connect:
# 	sudo docker exec -i  airflow_dbt_airflow-webserver_1 \bash -c "airflow connections add 'dbt_postgres_instance_raw_data' --conn-uri '$DBT_POSTGRESQL_CONN'"

stop:
	sudo docker-compose down

destroy:
	sudo docker system prune -a

teste:
	sudo docker network create postgres-network	