# Презентация проекта: Weather EL + dbt + Elementary

## 1. Проблема и цель
- Собирать снапшоты погоды (источник: API или mock fallback) и строить витрину для аналитики.

## 2. Архитектура (high-level)
- Python service (FastAPI) генерирует/получает данные и пишет в MongoDB.
- Airflow:
  - `weather_ingest` — регулярная генерация нагрузки (вызов сервиса)
  - `el_weather` — EL: Mongo -> Postgres `public.weather_raw` (JSONB) -> dbt (STG/ODS/DM) -> Elementary report
- Postgres:
  - Landing: `public.weather_raw` (сырые JSONB)
  - STG: `stg.stg_weather`
  - ODS: `ods.ods_weather_incremental`, `ods.ods_weather_merge`
  - DM: `dm.dm_weather_daily`
- Elementary:
  - dbt tests + anomaly tests
  - HTML отчёт (edr report)

## 3. EL вместо ETL
- Python не делает трансформаций: только пишет raw JSON.
- Все преобразования (типизация/агрегации) делаются в dbt.

## 4. Контроль качества
- dbt-core тесты: not_null, unique, relationships, accepted_values.
- elementary-data тесты: volume/freshness/event_freshness/column/all_columns/dimension anomalies.

## 5. Инсайты (пример)
- Динамика средней температуры по дням.
- Дни с низким `observations_count` как индикатор деградации пайплайна.

## 6. Артефакты для проверки
- Swagger: `http://<host>:8000/docs`
- Airflow: `http://<host>:8080` (admin/admin)
- MongoDB: `mongodb://weather:weather@<host>:27017/weather?authSource=admin`
- PostgreSQL: `postgresql://airflow:airflow@<host>:5432/analytics`
- Elementary report: `http://<host>:8081/elementary_report.html`

---

Экспорт в PDF (например):

```bash
pandoc presentation/presentation.md -o presentation/presentation.pdf
```
