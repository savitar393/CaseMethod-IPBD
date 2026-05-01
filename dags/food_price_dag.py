import sys
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.append("/opt/airflow/etl")

from food_price_pipeline import (
    extract_to_minio,
    transform_from_minio,
    load_to_postgres,
    calculate_alerts,
)


default_args = {
    "owner": "panganwatch",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="panganwatch_food_price_pipeline",
    description="ETL pipeline for food price monitoring and alert calculation",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["panganwatch", "food-price", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_raw_data_to_minio",
        python_callable=extract_to_minio,
    )

    transform_task = PythonOperator(
        task_id="transform_data_from_minio",
        python_callable=transform_from_minio,
    )

    load_task = PythonOperator(
        task_id="load_processed_data_to_postgres",
        python_callable=load_to_postgres,
    )

    alert_task = PythonOperator(
        task_id="calculate_price_alerts",
        python_callable=calculate_alerts,
    )

    extract_task >> transform_task >> load_task >> alert_task