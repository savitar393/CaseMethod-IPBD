FROM apache/airflow:3.1.8

USER airflow

RUN pip install --no-cache-dir \
    psycopg2-binary \
    pandas \
    requests \
    beautifulsoup4