# Weather EL (Airflow + dbt + Elementary)

Production-style EL pipeline for the ITMO “Data Managing” course.

- Python web service (FastAPI) stores **raw JSON** snapshots in MongoDB.
- Airflow generates load and runs an **EL** pipeline: Mongo → Postgres landing JSONB.
- dbt does parsing + transformations in SQL (STG → ODS → DM).
- Elementary runs observability tests + generates an HTML report.

## URLs (local)

- Airflow UI: http://localhost:8080 (admin/admin)
- Weather service Swagger: http://localhost:8000/docs
- Elementary report: http://localhost:8081/elementary_report.html

## Quickstart

```bash
docker compose up -d --build
```

Then unpause and run the pipeline:

```bash
docker compose exec -T airflow-scheduler airflow dags unpause weather_ingest
docker compose exec -T airflow-scheduler airflow dags unpause el_weather
docker compose exec -T airflow-scheduler airflow dags trigger el_weather
```

## Proof links (rubric checklist)

Each requirement below links to the exact code/config section where it is implemented.

### 1) Python web service (no infinite loop) + Swagger

- Service is FastAPI (HTTP endpoints, not a `while True` loop): [src/app.py#L1-L6](src/app.py#L1-L6), [src/app.py#L92-L128](src/app.py#L92-L128)
- Health endpoint (for readiness checks): [src/app.py#L122-L125](src/app.py#L122-L125)
- Ingest endpoint that writes to Mongo: [src/app.py#L127-L189](src/app.py#L127-L189)
- Container entrypoint runs `uvicorn`: [Dockerfile#L18-L20](Dockerfile#L18-L20)

### 2) MongoDB used as raw storage

- Mongo container + credentials: [docker-compose.yml#L33-L46](docker-compose.yml#L33-L46)
- Service connects to Mongo (envs): [docker-compose.yml#L144-L165](docker-compose.yml#L144-L165)
- Mongo write (insert raw payload): [src/app.py#L155-L163](src/app.py#L155-L163)

### 3) Airflow orchestration (generation + EL pipeline)

- Load generation DAG (calls the service on schedule): [dags/weather_ingest.py#L21-L40](dags/weather_ingest.py#L21-L40)
- EL DAG (Mongo → Postgres landing, then dbt + tests + report): [dags/etl_weather.py#L87-L135](dags/etl_weather.py#L87-L135)
- Interval-based extraction (idempotent by Airflow data interval): [dags/etl_weather.py#L34-L49](dags/etl_weather.py#L34-L49)

### 4) PostgreSQL landing table (JSONB) + EL (raw JSON only)

- Landing table DDL (`payload JSONB`): [sql/weather_schema.sql#L4-L10](sql/weather_schema.sql#L4-L10)
- Upsert of raw JSON into Postgres (no parsing in Python): [dags/etl_weather.py#L52-L85](dags/etl_weather.py#L52-L85)
- Airflow init also ensures landing table exists (idempotent bootstrap): [docker-compose.yml#L84-L110](docker-compose.yml#L84-L110)

### 5) dbt transformations (SQL) + layered models (STG/ODS/DM)

- Source definition for the landing table: [dbt/models/sources.yml#L1-L22](dbt/models/sources.yml#L1-L22)
- STG parses JSONB into typed columns: [dbt/models/staging/stg_weather.sql#L1-L12](dbt/models/staging/stg_weather.sql#L1-L12)
- ODS incremental #1 (standard incremental): [dbt/models/ods/ods_weather_incremental.sql#L1-L26](dbt/models/ods/ods_weather_incremental.sql#L1-L26)
- ODS incremental #2 (merge strategy): [dbt/models/ods/ods_weather_merge.sql#L1-L26](dbt/models/ods/ods_weather_merge.sql#L1-L26)
- DM mart with CTE + window function: [dbt/models/marts/dm_weather_daily.sql#L7-L32](dbt/models/marts/dm_weather_daily.sql#L7-L32)
- dbt project schemas per layer: [dbt/dbt_project.yml#L17-L27](dbt/dbt_project.yml#L17-L27)

### 6) Data quality: dbt tests (4 types) + custom test

- `unique` / `not_null` / `relationships` / `accepted_values`: [dbt/models/staging/schema.yml#L8-L33](dbt/models/staging/schema.yml#L8-L33)
- Custom generic test `valid_temperature`: [dbt/tests/generic/test_valid_temperature.sql#L1-L3](dbt/tests/generic/test_valid_temperature.sql#L1-L3)
- Custom test used in DM schema: [dbt/models/marts/schema.yml#L13-L32](dbt/models/marts/schema.yml#L13-L32)

### 7) Elementary (6 types of monitors) + HTML report

- Elementary package installed: [dbt/packages.yml#L1-L3](dbt/packages.yml#L1-L3)
- ODS model includes 6 Elementary tests (volume/freshness/event_freshness/column/all_columns/dimension anomalies): [dbt/models/ods/schema.yml#L3-L35](dbt/models/ods/schema.yml#L3-L35)
- Airflow task generates report via `edr report`: [dags/etl_weather.py#L126-L130](dags/etl_weather.py#L126-L130)
- Report is served from `dbt/target` by a lightweight HTTP server container: [docker-compose.yml#L166-L175](docker-compose.yml#L166-L175)

### 8) Notebook + exported artifact

- Notebook: [notebooks/weather_mart_insights.ipynb](notebooks/weather_mart_insights.ipynb)
- HTML export (committed artifact): [notebooks/weather_mart_insights.html](notebooks/weather_mart_insights.html)
- Export command documented: [notebooks/README.md#L16-L20](notebooks/README.md#L16-L20)

### 9) Presentation artifact (committed) + reproducible generator

- Presentation deck source: [presentation/presentation.md](presentation/presentation.md)
- Generated presentation artifact: [presentation/presentation.pptx](presentation/presentation.pptx)
- Generator script (reproducible build): [scripts/generate_presentation_pptx.py#L1-L101](scripts/generate_presentation_pptx.py#L1-L101)

### 10) Code quality tooling + CI

- pre-commit hooks (ruff + formatting + sqlfmt for dbt SQL): [.pre-commit-config.yaml#L1-L26](.pre-commit-config.yaml#L1-L26)
- CI runs pre-commit on push/PR: [.github/workflows/ci.yml#L1-L24](.github/workflows/ci.yml#L1-L24)

### 11) CI/CD deploy pipeline (GitHub Actions)

- Auto-deploy workflow (bundle → remote `docker compose up -d --build` + smoke checks + post-deploy DAG runs): [.github/workflows/deploy.yml#L1-L220](.github/workflows/deploy.yml#L1-L220)

### 12) “No push to main” requirement (repo setting)

This is enforced in Git hosting settings (not in code).

- Enable **Branch protection** for `main` (require PR, disallow direct pushes).
- CI check to require before merge: [.github/workflows/ci.yml#L1-L24](.github/workflows/ci.yml#L1-L24)

## Connection strings (local)

- MongoDB: `mongodb://weather:weather@localhost:27017/weather?authSource=admin`
- Postgres analytics DB: `postgresql://airflow:airflow@localhost:5432/analytics`

## Notes

- dbt docs/commands and structure are described in [dbt/README.md](dbt/README.md).
- If you want fully reproducible deployed URLs, replace `localhost` with your server IP/host.
