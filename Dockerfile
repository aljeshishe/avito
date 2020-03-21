FROM aljeshishe/airflow

USER root


RUN apt update && apt install openjdk-8-jre-headless  ca-certificates-java -y
COPY requirements.txt requirements.txt
RUN pwd  && pip install -r requirements.txt


ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]