FROM apache/airflow:3.1.0-python3.12

COPY requirements.txt /opt/airflow/

USER root
RUN apt-get update && apt-get install -y build-essential

USER airflow

# Install FAB auth manager first
RUN pip install apache-airflow[celery]==3.1.0
RUN pip install --no-cache-dir apache-airflow-providers-fab
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt