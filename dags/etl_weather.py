from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "el_weather"
MONGO_CONN_ID = "mongo_weather"
POSTGRES_CONN_ID = "postgres_weather"
MONGO_COLLECTION = "weather_raw"
TARGET_TABLE = "weather_raw"
DBT_PROJECT_DIR = "/opt/airflow/dbt"
WEATHER_SERVICE_URL = "http://weather-service:8000"


def trigger_weather_ingest() -> Dict[str, Any]:
    """Call the weather service once to generate a new MongoDB document."""

    response = requests.post(f"{WEATHER_SERVICE_URL}/ingest", timeout=30)
    response.raise_for_status()
    return response.json()


def extract_documents(**context: Any) -> List[Dict[str, Any]]:
    """Pull raw weather snapshots for the scheduled interval from MongoDB."""

    # Airflow schedules DAG runs using a logical data interval; we must query Mongo by that window
    # to keep EL idempotent and reproducible.
    interval_start = context["data_interval_start"].to_iso8601_string()  # type: ignore[attr-defined]
    interval_end = context["data_interval_end"].to_iso8601_string()  # type: ignore[attr-defined]

    mongo_hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    collection = mongo_hook.get_collection(mongo_collection=MONGO_COLLECTION)

    query = {"fetched_at": {"$gte": interval_start, "$lt": interval_end}}
    documents = list(collection.find(query))
    for doc in documents:
        doc["_id"] = str(doc.get("_id"))  # make JSON serializable for XCom
    return documents


def load_rows(ti: Any) -> None:  # pylint: disable=invalid-name
    """Load raw JSON documents from MongoDB into PostgreSQL landing table (EL only)."""

    documents: List[Dict[str, Any]] = ti.xcom_pull(task_ids="extract_from_mongo") or []
    if not documents:
        return

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} (
            mongo_id,
            fetched_at,
            payload
        ) VALUES (%s, %s, %s::jsonb)
        ON CONFLICT (mongo_id)
        DO UPDATE SET
            fetched_at = EXCLUDED.fetched_at,
            payload = EXCLUDED.payload,
            updated_at = NOW();
    """

    # Upsert-by-mongo_id makes repeated DAG runs safe (retries/backfills won't duplicate rows).

    for doc in documents:
        fetched_at_raw = doc.get("fetched_at")
        if not fetched_at_raw:
            continue
        fetched_at = datetime.fromisoformat(str(fetched_at_raw))
        mongo_id = str(doc.get("_id"))
        pg_hook.run(
            insert_sql,
            parameters=(mongo_id, fetched_at, json.dumps(doc, ensure_ascii=False)),
        )


def create_dag() -> DAG:
    with DAG(
        dag_id=DAG_ID,
        description="EL pipeline: load raw Mongo JSON into Postgres, then transform via dbt",
        schedule_interval="@hourly",
        start_date=days_ago(1),
        catchup=False,
        max_active_runs=1,
        default_args={"owner": "data-platform"},
        tags=["weather", "el"],
    ) as dag:
        start = EmptyOperator(task_id="start")

        ingest = PythonOperator(
            task_id="trigger_weather_ingest",
            python_callable=trigger_weather_ingest,
        )

        extract = PythonOperator(
            task_id="extract_from_mongo",
            python_callable=extract_documents,
        )

        load = PythonOperator(
            task_id="load_postgres",
            python_callable=load_rows,
        )

        dbt_run = BashOperator(
            task_id="dbt_run",
            # Keep transformations in dbt (EL pipeline): Python only loads raw JSON into Postgres.
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps && dbt run --select elementary --full-refresh && dbt run",
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
        )

        edr_report = BashOperator(
            task_id="edr_report",
            # HTML report is written into /opt/airflow/dbt/target and served by the elementary-report container.
            bash_command=f"cd {DBT_PROJECT_DIR} && edr report --file-path /opt/airflow/dbt/target/elementary_report.html --profiles-dir .",
        )

        end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

        start >> ingest >> extract >> load >> dbt_run >> dbt_test >> edr_report >> end

    return dag


globals()[DAG_ID] = create_dag()
