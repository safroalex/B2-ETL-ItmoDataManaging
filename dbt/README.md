# dbt проект (weather_dbt)

## Структура

- `models/sources.yml` — источники (landing таблица `public.weather_raw`)
- `models/staging/` — STG слой (`stg_weather`): извлекает поля из JSONB `payload` и приводит типы
- `models/ods/` — ODS слой:
  - `ods_weather_incremental` — incremental (delete+insert / append через `is_incremental()` фильтр)
  - `ods_weather_merge` — incremental strategy `merge`
- `models/marts/` — DM слой:
  - `dm_weather_daily` — дневная витрина

## Запуск локально (через контейнер Airflow)

```bash
docker compose exec -T airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt deps && dbt run && dbt test"
```

## Elementary report

Отчёт генерируется командой:

```bash
docker compose exec -T airflow-scheduler bash -lc "cd /opt/airflow/dbt && edr report --file-path target/elementary_report.html --profiles-dir ."
```

И доступен по URL:

- http://localhost:8081/elementary_report.html
