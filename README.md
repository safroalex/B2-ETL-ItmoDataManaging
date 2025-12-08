## Overview

This repository contains Part One of the Itmo Data Managing course project: a production-ready ETL environment that continuously ingests current weather data from the Yandex Weather API, stores the raw payloads in MongoDB, and loads curated metrics into PostgreSQL via Apache Airflow. Docker Compose orchestrates every dependency so the system can be reproduced locally or on a remote server.

## Architecture

| Component | Role |
|-----------|------|
| **Weather ingestion service** (`src/app.py`) | Polls the Yandex Weather API on a fixed cadence, enriches each payload with `fetched_at`, and writes documents into MongoDB.
| **MongoDB** | Durable store for the raw weather snapshots (`weather.weather_raw` collection).
| **PostgreSQL** | Hosts both the Airflow metadata database and the analytics schema (`analytics.weather_analytics`).
| **Apache Airflow** | Runs on the LocalExecutor, orchestrating the hourly extract–transform–load pipeline.
| **Docker Compose** | Spins up MongoDB, PostgreSQL, the Airflow components (init, scheduler, webserver), and the weather service in a single command.

## Repository Artifacts

| Path | Description |
|------|-------------|
| `docker-compose.yml` | Defines every container, shared networks, and bootstrap workflow (Airflow init, scheduler, webserver, weather service). |
| `Dockerfile` | Builds the weather service image (Python 3.11, dependencies from `src/requirements.txt`). |
| `Dockerfile.airflow` | Extends `apache/airflow:2.9.2-python3.11`, installs provider requirements, and runs as the `airflow` user. |
| `airflow.env` | Provides UID/GID overrides so Airflow can write host-mounted logs. |
| `dags/etl_weather.py` | Source of the Airflow DAG that moves data from MongoDB to PostgreSQL. |
| `sql/weather_schema.sql` | Initializes the `analytics` database and the `weather_analytics` fact table with triggers. |
| `src/app.py` | Weather ingestion loop with graceful shutdown and structured logging. |
| `requirements/airflow.txt` | Python packages baked into the Airflow image (Mongo + Postgres providers). |
| `logs/.gitkeep` & `plugins/.gitkeep` | Empty placeholders so Airflow volume mounts resolve inside the containers. |

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
   - MongoDB: `mongodb://weather:weather@localhost:27017/weather?authSource=admin`
   - PostgreSQL analytics DB: `postgresql://airflow:airflow@localhost:5432/analytics`

Bootstrap actions (Airflow DB migration, admin user creation, connection setup, and analytics schema provisioning) happen automatically during the `airflow-init` run.

## Verifying the ETL Pipeline

1. **Weather service** – tail logs with `docker compose logs -f weather-service` and ensure messages such as `Stored weather snapshot _id=...` appear every polling cycle.
2. **MongoDB raw layer** – check document growth:
   ```bash
   docker compose exec mongodb \
     mongosh --quiet --username weather --password weather --authenticationDatabase admin \
     --eval "db.getSiblingDB('weather').weather_raw.countDocuments()"
   ```
3. **Airflow DAG availability** – open the Airflow UI, confirm `etl_weather` is in the DAG list, and unpause it (or run `docker compose exec airflow-scheduler airflow dags unpause etl_weather`).
4. **Manual validation run** – trigger the DAG via the UI or CLI:
   ```bash
   docker compose exec airflow-scheduler airflow dags trigger etl_weather
   ```
   Monitor task logs to ensure `extract_from_mongo`, `transform_weather`, and `load_postgres` all reach `success`.
5. **Analytics output** – query PostgreSQL to confirm new rows:
   ```bash
   docker compose exec postgres \
     psql -U airflow -d analytics \
     -c "SELECT * FROM weather_analytics ORDER BY observed_at DESC LIMIT 5;"
   ```
   Timestamps and metrics should line up with the raw Mongo documents from step 2.

Once the DAG is unpaused, the scheduler continues to execute it hourly without manual intervention.

## Deploying on a Server

1. Install Docker Engine + Compose on the target host and clone this repository.
2. Set environment-specific secrets (API key, Airflow admin password) via `.env` or a secrets manager.
3. Adjust published ports or attach the containers to existing networks if necessary.
4. Launch the stack with `docker compose up -d --build` and expose the Airflow UI through HTTPS with authentication (VPN, reverse proxy, or security groups).
5. Point monitoring/alerting at the Airflow scheduler logs and database health checks.

### GitHub Actions deployment pipeline

- Workflow: `.github/workflows/deploy.yml` (runs on pushes to `main` or `feature/partOne` and on manual dispatch).
- Secrets required:
   - `DEPLOY_SSH_KEY` – private key for the `cybrex@213.165.34.52` account.
   - `DEPLOY_SUDO_PASSWORD` – password for the same user so `sudo`-guarded Docker commands can run non-interactively.
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

