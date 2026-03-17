FROM apache/airflow:2.8.0
USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk
USER airflow
RUN pip install pyspark