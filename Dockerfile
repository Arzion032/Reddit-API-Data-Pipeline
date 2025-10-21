FROM apache/airflow:3.1.2-python3.12

USER root

RUN apt-get update && apt-get install -y build-essential

USER airflow
# Install Celery extras (if not already included)
RUN pip install apache-airflow[celery]==3.1.2
