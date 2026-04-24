FROM apache/airflow:3.1.8

RUN pip install --no-cache-dir requests apache-airflow-providers-postgres