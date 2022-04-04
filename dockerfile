FROM amazonlinux

RUN yum update -y && \
  yum install -y python3 git aws-cli

RUN pip3 install pip install dbt==0.15 && \
    pip3 install wtforms==2.3.3 && \
    pip3 install 'apache-airflow[postgres]==1.10.14' && \
    pip3 install SQLAlchemy==1.3.23 && \
    pip3 install markupsafe==2.0.1 && \
    pip3 install Flask==1.1.4

RUN mkdir /project
COPY scripts_airflow/ /project/scripts/

RUN chmod +x /project/scripts/init.sh
ENTRYPOINT [ "/project/scripts/init.sh" ]
