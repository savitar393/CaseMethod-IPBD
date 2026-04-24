from datetime import datetime

import requests
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


API_URL = "http://host.docker.internal:8000/articles"

# If Airflow is not running in Docker, use:
# API_URL = "http://127.0.0.1:8000/articles"


def fetch_articles_from_api(**context):
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    articles = response.json()

    if not isinstance(articles, list):
        raise ValueError("API response must be a JSON list")

    if len(articles) == 0:
        raise ValueError("No articles returned from API")

    context["ti"].xcom_push(key="raw_articles", value=articles)


def transform_articles(**context):
    raw_articles = context["ti"].xcom_pull(
        key="raw_articles",
        task_ids="fetch_articles_from_api"
    )

    transformed = []

    for article in raw_articles:
        scraped_at_raw = article.get("scraped_at")

        try:
            scraped_at = pendulum.parse(scraped_at_raw).to_datetime_string()
        except Exception:
            scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        transformed.append({
            "title": article.get("title"),
            "url": article.get("url"),
            "description": article.get("description"),
            "author": article.get("author"),
            "scraped_at": scraped_at,
            "source": article.get("source", "Wired.com"),
        })

    context["ti"].xcom_push(key="transformed_articles", value=transformed)


def load_articles_to_postgres(**context):
    articles = context["ti"].xcom_pull(
        key="transformed_articles",
        task_ids="transform_articles"
    )

    postgres = PostgresHook(postgres_conn_id="wired_postgres")

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS wired_articles (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        url TEXT UNIQUE NOT NULL,
        description TEXT,
        author TEXT,
        scraped_at TIMESTAMP,
        source TEXT
    );
    """

    insert_sql = """
    INSERT INTO wired_articles 
        (title, url, description, author, scraped_at, source)
    VALUES 
        (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (url) DO UPDATE SET
        title = EXCLUDED.title,
        description = EXCLUDED.description,
        author = EXCLUDED.author,
        scraped_at = EXCLUDED.scraped_at,
        source = EXCLUDED.source;
    """

    conn = postgres.get_conn()
    cursor = conn.cursor()

    cursor.execute(create_table_sql)

    for article in articles:
        if article["title"] and article["url"]:
            cursor.execute(
                insert_sql,
                (
                    article["title"],
                    article["url"],
                    article["description"],
                    article["author"],
                    article["scraped_at"],
                    article["source"],
                )
            )

    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    dag_id="wired_articles_pipeline",
    start_date=pendulum.datetime(2026, 4, 1, tz="Asia/Jakarta"),
    schedule=None,
    catchup=False,
    tags=["responsi", "wired", "airflow"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_articles_from_api",
        python_callable=fetch_articles_from_api,
    )

    transform_task = PythonOperator(
        task_id="transform_articles",
        python_callable=transform_articles,
    )

    load_task = PythonOperator(
        task_id="load_articles_to_postgres",
        python_callable=load_articles_to_postgres,
    )

    fetch_task >> transform_task >> load_task