## Overview

This repository contains the Itmo Data Managing course project: a production-ready ELT environment where a Python **web service** fetches weather data from the Yandex Weather API and writes raw documents into MongoDB. Apache Airflow then (1) triggers the service on a schedule to generate load and (2) runs an **EL** pipeline that copies raw JSON documents from MongoDB into PostgreSQL. Data parsing/cleaning and analytics marts are done in PostgreSQL via dbt, with data quality monitoring via Elementary.

## Architecture

| Component | Role |
|-----------|------|
| **Weather ingestion service** (`src/app.py`) | HTTP service with Swagger (`/docs`) that fetches a single weather snapshot from Yandex Weather API and writes it into MongoDB.
| **MongoDB** | Durable store for the raw weather snapshots (`weather.weather_raw` collection).
| **PostgreSQL** | Hosts both the Airflow metadata database and the analytics database. Raw docs are loaded into `analytics.public.weather_raw` (JSONB), then transformed by dbt.
| **Apache Airflow** | Runs on the LocalExecutor, orchestrating (a) frequent load generation (calling the service) and (b) an hourly EL pipeline and dbt runs.
| **dbt** | Transforms data in PostgreSQL (STG -> ODS -> DM) and runs data quality tests via Elementary.
| **Docker Compose** | Spins up MongoDB, PostgreSQL, the Airflow components (init, scheduler, webserver), and the weather service in a single command.

## Repository Artifacts

| Path | Description |
|------|-------------|
| `docker-compose.yml` | Defines every container, shared networks, and bootstrap workflow (Airflow init, scheduler, webserver, weather service). |
| `Dockerfile` | Builds the weather service image (Python 3.11, dependencies from `src/requirements.txt`). |
| `Dockerfile.airflow` | Extends `apache/airflow:2.9.2-python3.11`, installs provider requirements, and runs as the `airflow` user. |
| `airflow.env` | Provides UID/GID overrides so Airflow can write host-mounted logs. |
| `dags/weather_ingest.py` | Airflow DAG that calls the weather service on a schedule (load generation). |
| `dags/etl_weather.py` | Airflow DAG `el_weather` that copies raw Mongo JSON into Postgres and runs dbt + Elementary. |
| `sql/weather_schema.sql` | Initializes the `analytics` database and the raw landing table `weather_raw` (JSONB) with triggers. |
| `src/app.py` | Weather ingestion loop with graceful shutdown and structured logging. |
| `requirements/airflow.txt` | Python packages baked into the Airflow image (Mongo + Postgres providers). |
| `logs/.gitkeep` & `plugins/.gitkeep` | Empty placeholders so Airflow volume mounts resolve inside the containers. |
| `dbt/` | Contains the dbt project (models, profiles, tests) for data transformation and quality monitoring. |

## Part 2: DBT & Elementary Integration

The project has been extended with **dbt** for data transformation and **Elementary** for data observability.

### DBT Project Structure (`dbt/`)

The dbt project is configured to run against the PostgreSQL `analytics` database.

- **Models**:
   - `staging/stg_weather`: View over the raw `weather_raw` JSONB landing table.
  - `ods/ods_weather_incremental`: Incremental table using `delete+insert` strategy (standard incremental).
  - `ods/ods_weather_merge`: Incremental table using `merge` strategy (Postgres 15+).
  - `marts/dm_weather_daily`: Daily aggregation of weather metrics.
- **Tests**:
  - Basic schema tests (unique, not null) defined in `sources.yml`.
  - Elementary data quality monitoring enabled via `packages.yml`.

### Airflow Integration

Airflow runs two DAGs:
1. `weather_ingest` (every 10 minutes): calls the weather service and writes a new document into MongoDB.
2. `el_weather` (hourly): copies raw JSON documents from MongoDB into PostgreSQL and then runs dbt + tests + Elementary report.

### Elementary Dashboard

The project includes a lightweight server to view the Elementary data quality report.
After the DAG runs successfully (specifically the `edr_report` task), you can access the dashboard at:

**<http://localhost:8081/elementary_report.html>**

This report provides a visual interface for test results, data lineage, and anomaly detection.

## Local Prerequisites

- Docker Desktop 4.x (or Docker Engine 24+ plus the Compose plugin)
- Open TCP ports 27017 (MongoDB), 5432 (PostgreSQL), and 8080 (Airflow UI)
- Outbound HTTPS access to `api.weather.yandex.ru`

## Configuration

All defaults are suitable for local testing and can be overridden through Compose environment variables or a `.env` file when needed.

| Variable | Default | Notes |
|----------|---------|-------|
| `YANDEX_API_KEY` | Provided demo key | Replace with your personal key before public deployment. |
| `POLLING_INTERVAL_SECONDS` | `600` | Sets the weather service polling loop. |
| `MONGO_URI`, `MONGO_DB`, `MONGO_COLLECTION` | Point to the bundled MongoDB | Keep aligned with Airflow connection `mongo_weather`. |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | PostgreSQL DSN | Configured for the co-located PostgreSQL container. |
| Airflow connections | Created by `airflow-init` | `mongo_weather` uses `authSource=admin`; `postgres_weather` targets the analytics schema. |

## Running the Stack Locally

1. **Build and start**
   ```bash
   docker compose up --build -d
   ```
2. **Confirm container health**
   ```bash
   docker compose ps
   ```
3. **Access the services**
   - Airflow UI: <http://localhost:8080> (user `admin`, password `admin`)
   - Weather service Swagger: <http://localhost:8000/docs>
   - MongoDB: `mongodb://weather:weather@localhost:27017/weather?authSource=admin`
   - PostgreSQL analytics DB: `postgresql://airflow:airflow@localhost:5432/analytics`

Bootstrap actions (Airflow DB migration, admin user creation, connection setup, and analytics schema provisioning) happen automatically during the `airflow-init` run.

## Verifying the ETL Pipeline

1. **Weather service** – open Swagger at <http://localhost:8000/docs> and call `POST /ingest` (or rely on the `weather_ingest` DAG).
2. **MongoDB raw layer** – check document growth:
   ```bash
   docker compose exec mongodb \
     mongosh --quiet --username weather --password weather --authenticationDatabase admin \
     --eval "db.getSiblingDB('weather').weather_raw.countDocuments()"
   ```
3. **Airflow DAG availability** – open the Airflow UI, confirm `weather_ingest` and `el_weather` are in the DAG list, and unpause them.
4. **Manual validation run** – trigger the DAG via the UI or CLI:
   ```bash
   docker compose exec airflow-scheduler airflow dags trigger el_weather
   ```
   Monitor task logs to ensure `extract_from_mongo` and `load_postgres` reach `success`.
5. **Analytics output** – query PostgreSQL to confirm new rows:
   ```bash
   docker compose exec postgres \
     psql -U airflow -d analytics \
     -c "SELECT * FROM weather_raw ORDER BY fetched_at DESC LIMIT 5;"
   ```
   Raw JSON should be present in `payload`.
6. **DBT Models** – check the created tables in Postgres:
   ```bash
   docker compose exec postgres \
     psql -U airflow -d analytics \
     -c "SELECT table_name FROM information_schema.tables WHERE table_schema IN ('dbt_stg', 'dbt_ods', 'dbt_dm');"
   ```
   You should see `stg_weather`, `ods_weather_incremental`, `ods_weather_merge`, and `dm_weather_daily`.

7. **Elementary Reports** – Elementary tables are created in the `dbt_elementary` schema (or similar, depending on config). You can check for test results:
   ```bash
   docker compose exec postgres \
     psql -U airflow -d analytics \
     -c "SELECT count(*) FROM dbt_elementary.elementary_test_results;"
   ```

Once the DAG is unpaused, the scheduler continues to execute it hourly without manual intervention.

## Deploying on a Server

1. Install Docker Engine + Compose on the target host and clone this repository.
2. Set environment-specific secrets (API key, Airflow admin password) via `.env` or a secrets manager.
3. Adjust published ports or attach the containers to existing networks if necessary.
4. Launch the stack with `docker compose up -d --build` and expose the Airflow UI through HTTPS with authentication (VPN, reverse proxy, or security groups).
5. Point monitoring/alerting at the Airflow scheduler logs and database health checks.

### GitHub Actions deployment pipeline

- Workflow: `.github/workflows/deploy.yml` (runs on pushes to `main` or `feature/partOne` and on manual dispatch).
- Secrets required: `DEPLOY_SSH_KEY` – private key for the `cybrex@213.165.34.52` account.
- Remote path: `/opt/b2-etl-itmodata` (created automatically if missing).
- Behavior: packs the repository, securely copies it to the server, and runs `docker compose down && docker compose up -d --build` to refresh the stack.

Before enabling the workflow, provision the deployment user and SSH keys as described in `docs/server_setup.md`.

## Operations & Troubleshooting

- Weather service logs: `docker compose logs -f weather-service`
- Airflow scheduler & DAG logs: `docker compose logs -f airflow-scheduler`
- Re-run the initialization bootstrap (connections, DB migrations, variables): `docker compose run --rm airflow-init`
- Reset the environment completely (including Mongo/Postgres volumes): `docker compose down -v`

## Security Notes

- Store the Yandex API key and Airflow credentials outside the repository (Compose `.env`, Docker secrets, Vault, etc.).
- Rotate credentials regularly and change the default Airflow admin password before exposing the UI beyond localhost.
- Restrict ingress to MongoDB and PostgreSQL when deploying in shared or cloud environments.

## Required Artifact URLs

Fill these in for the deployed server (examples assume host `62.60.228.129`):

- Swagger URL: http://62.60.228.129:8000/docs
- MongoDB URL: mongodb://weather:weather@62.60.228.129:27017/weather?authSource=admin
- PostgreSQL URL: postgresql://airflow:airflow@62.60.228.129:5432/analytics
- Airflow:
   - URL: http://62.60.228.129:8080/home
   - User: admin
   - Password: admin
- Elementary edr report URL: http://62.60.228.129:8081/elementary_report.html
