import os
import subprocess
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
from news_pipeline import load_news_to_postgres

default_args = {
    "owner": "panganwatch",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def run_python_script(script_path, extra_env=None):
    env = os.environ.copy()

    if extra_env:
        env.update(extra_env)

    result = subprocess.run(
        ["python", script_path],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    print(result.stdout)

    if result.stderr:
        print(result.stderr)

def scrape_current_food_price():
    command = ["python", "/opt/airflow/scraper/scrape_ipj_rendered.py"]

    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )

    print(result.stdout)

    if result.stderr:
        print(result.stderr)


def scrape_current_food_price():
    run_python_script("/opt/airflow/scraper/scrape_ipj_rendered.py")


def scrape_pihps_chart_history():
    run_python_script("/opt/airflow/scraper/scrape_pihps_chart_history.py")

def scrape_pihps_grid_backfill():
    subprocess.run(
        [
            "python",
            "/opt/airflow/scraper/scrape_pihps_grid_backfill.py"
        ],
        check=True,
        env={
            **os.environ,
            "PIHPS_DAYS_BACK": "7",
        },
    )

def scrape_food_news():
    run_python_script("/opt/airflow/scraper/scrape_food_news.py")

def load_food_news():
    run_python_script("/opt/airflow/etl/news_pipeline.py")

def scrape_bps_inflation():
    run_python_script("/opt/airflow/scraper/scrape_bps_inflation.py")

def load_inflation_data():
    run_python_script("/opt/airflow/etl/inflation_pipeline.py")

def scrape_bmkg_weather():
    run_python_script("/opt/airflow/scraper/scrape_bmkg_weather.py")

def load_weather_data():
    run_python_script("/opt/airflow/etl/weather_pipeline.py")

def scrape_bi_jisdor():
    run_python_script("/opt/airflow/scraper/scrape_bi_jisdor.py")

def load_macro_cost_data():
    run_python_script("/opt/airflow/etl/macro_cost_pipeline.py")

def scrape_climate_data():
    run_python_script("/opt/airflow/scraper/scrape_nasa_power_climate.py")

def load_climate_data():
    run_python_script("/opt/airflow/etl/climate_pipeline.py")

def preprocess_food_price():
    run_python_script("/opt/airflow/etl/preprocess_food_price.py")

def build_combined_real_price():
    subprocess.run(
        ["python", "/opt/airflow/scraper/build_combined_real_price.py"],
        check=True
    )


with DAG(
    dag_id="panganwatch_food_price_pipeline",
    description="Scrape, extract, transform, load, and analyze food price data",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["panganwatch", "food-price", "scraping", "etl"],
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_ipj_and_berita_jakarta",
        python_callable=scrape_current_food_price,
    )

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

    scrape_pihps_chart_task = PythonOperator(
        task_id="scrape_pihps_chart_history",
        python_callable=scrape_pihps_chart_history,
    )

    scrape_pihps_grid_task = PythonOperator(
        task_id="scrape_pihps_grid_backfill",
        python_callable=scrape_pihps_grid_backfill,
    )

    scrape_news_task = PythonOperator(
        task_id="scrape_food_news",
        python_callable=scrape_food_news,
    )

    load_news_task = PythonOperator(
        task_id="load_food_news_to_postgres",
        python_callable=load_food_news,
    )

    scrape_inflation_task = PythonOperator(
        task_id="scrape_bps_inflation",
        python_callable=scrape_bps_inflation,
    )

    load_inflation_task = PythonOperator(
        task_id="load_inflation_data",
        python_callable=load_inflation_data,
    )

    scrape_weather_task = PythonOperator(
        task_id="scrape_bmkg_weather",
        python_callable=scrape_bmkg_weather,
    )

    load_weather_task = PythonOperator(
        task_id="load_weather_data",
        python_callable=load_weather_data,
    )

    scrape_jisdor_task = PythonOperator(
        task_id="scrape_bi_jisdor",
        python_callable=scrape_bi_jisdor,
    )

    load_macro_cost_task = PythonOperator(
        task_id="load_macro_cost_data",
        python_callable=load_macro_cost_data,
    )

    scrape_climate_task = PythonOperator(
        task_id="scrape_nasa_power_climate",
        python_callable=scrape_climate_data,
    )

    load_climate_task = PythonOperator(
        task_id="load_climate_data",
        python_callable=load_climate_data,
    )

    preprocess_task = PythonOperator(
        task_id="preprocess_food_price",
        python_callable=preprocess_food_price,
    )

    combine_task = PythonOperator(
        task_id="build_combined_real_price",
        python_callable=build_combined_real_price,
    )

    [scrape_task, scrape_pihps_grid_task] >> combine_task
    combine_task >> preprocess_task >> extract_task >> transform_task >> load_task >> alert_task
    scrape_news_task >> load_news_task
    scrape_inflation_task >> load_inflation_task
    scrape_weather_task >> load_weather_task
    scrape_jisdor_task >> load_macro_cost_task
    scrape_climate_task >> load_climate_task