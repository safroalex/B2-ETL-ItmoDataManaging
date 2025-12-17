from __future__ import annotations

from typing import Any, Dict

import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DAG_ID = "weather_ingest"
WEATHER_SERVICE_URL = "http://weather-service:8000"


def trigger_weather_ingest() -> Dict[str, Any]:
    response = requests.post(f"{WEATHER_SERVICE_URL}/ingest", timeout=30)
    response.raise_for_status()
    return response.json()


with DAG(
    dag_id=DAG_ID,
    description="Generate load: call the weather ingestion service to write a new document into MongoDB.",
    schedule_interval="*/10 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform"},
    tags=["weather", "load"],
) as dag:
    start = EmptyOperator(task_id="start")

    ingest = PythonOperator(
        task_id="trigger_weather_ingest",
        python_callable=trigger_weather_ingest,
    )

    end = EmptyOperator(task_id="end")

    start >> ingest >> end
