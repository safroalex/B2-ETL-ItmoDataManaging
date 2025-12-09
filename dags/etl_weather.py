from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "etl_weather"
MONGO_CONN_ID = "mongo_weather"
POSTGRES_CONN_ID = "postgres_weather"
MONGO_COLLECTION = "weather_raw"
TARGET_TABLE = "weather_analytics"
DBT_PROJECT_DIR = "/opt/airflow/dbt"


def extract_documents(**context: Any) -> List[Dict[str, Any]]:
    """Pull raw weather snapshots for the scheduled interval from MongoDB."""

    interval_start = context["data_interval_start"].to_iso8601_string()  # type: ignore[attr-defined]
    interval_end = context["data_interval_end"].to_iso8601_string()  # type: ignore[attr-defined]

    mongo_hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    collection = mongo_hook.get_collection(mongo_collection=MONGO_COLLECTION)

    query = {"fetched_at": {"$gte": interval_start, "$lt": interval_end}}
    documents = list(collection.find(query))
    for doc in documents:
        doc["_id"] = str(doc.get("_id"))  # make JSON serializable for XCom
    return documents


def transform_documents(ti: Any) -> List[Dict[str, Any]]:  # pylint: disable=invalid-name
    """Trim large payload to only required analytical attributes."""

    documents: List[Dict[str, Any]] = ti.xcom_pull(task_ids="extract_from_mongo") or []
    transformed_rows: List[Dict[str, Any]] = []

    for doc in documents:
        fact = doc.get("fact", {})
        fetched_at = doc.get("fetched_at")
        if not fetched_at:
            continue
        transformed_rows.append(
            {
                "observed_at": datetime.fromisoformat(fetched_at),
                "temperature_c": fact.get("temp"),
                "humidity": fact.get("humidity"),
                "pressure_mm": fact.get("pressure_mm"),
                "pressure_pa": fact.get("pressure_pa"),
            }
        )
    return transformed_rows


def load_rows(ti: Any) -> None:  # pylint: disable=invalid-name
    """Persist transformed snapshots into PostgreSQL fact table."""

    rows: List[Dict[str, Any]] = ti.xcom_pull(task_ids="transform_weather") or []
    if not rows:
        return

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} (
            observed_at,
            temperature_c,
            humidity,
            pressure_mm,
            pressure_pa
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (observed_at)
        DO UPDATE SET
            temperature_c = EXCLUDED.temperature_c,
            humidity = EXCLUDED.humidity,
            pressure_mm = EXCLUDED.pressure_mm,
            pressure_pa = EXCLUDED.pressure_pa;
    """

    for row in rows:
        pg_hook.run(
            insert_sql,
            parameters=(
                row["observed_at"],
                row.get("temperature_c"),
                row.get("humidity"),
                row.get("pressure_mm"),
                row.get("pressure_pa"),
            ),
        )


def create_dag() -> DAG:
    with DAG(
        dag_id=DAG_ID,
        description="Extract Mongo weather snapshots, trim, and load into Postgres",
        schedule_interval="@hourly",
        start_date=days_ago(1),
        catchup=False,
        max_active_runs=1,
        default_args={"owner": "data-platform"},
        tags=["weather", "etl"],
    ) as dag:
        start = EmptyOperator(task_id="start")

        extract = PythonOperator(
            task_id="extract_from_mongo",
            python_callable=extract_documents,
        )

        transform = PythonOperator(
            task_id="transform_weather",
            python_callable=transform_documents,
        )

        load = PythonOperator(
            task_id="load_postgres",
            python_callable=load_rows,
        )

        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps && dbt run --select elementary --full-refresh && dbt run",
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
        )

        edr_report = BashOperator(
            task_id="edr_report",
            bash_command=f"cd {DBT_PROJECT_DIR} && edr report --file-path /opt/airflow/dbt/target/elementary_report.html --profiles-dir .",
        )

        end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

        start >> extract >> transform >> load >> dbt_run >> dbt_test >> edr_report >> end

    return dag


globals()[DAG_ID] = create_dag()